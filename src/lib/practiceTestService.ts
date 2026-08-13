import { supabase } from "./supabaseClient";
import { ParsedAssessmentQuestion, TopicPracticeTest, TestAttemptRecord } from "../types";
import { getResolvedViewUrl } from "./storageService";
import { doc, setDoc, onSnapshot } from "firebase/firestore";
import { getFirebaseDb } from "./firebase";

import { safeLocalStorageSetItem, safeLocalStorageGetItem, safeLocalStorageRemoveItem } from "./safeStorage";
import { deleteTopicAttemptsFromPersistence } from "./testScorePersistence";

const TESTS_CACHE_KEY = "tuition_topic_practice_tests_bank";
const SYNC_QUEUE_KEY = "tuition_practice_tests_sync_queue";

const IDB_DB_NAME = "tuition_practice_tests_db";
const IDB_DB_VERSION = 1;
const IDB_SYNC_QUEUE_STORE = "syncQueue";
const MAX_SYNC_RETRIES = 3;
const MAX_LOCAL_STORAGE_ITEM_BYTES = 50 * 1024;

let memorySyncQueue: SyncQueueItem[] = [];
let practiceTestsRealtimeChannel: any = null;
let isRealtimeInitialized = false;

/**
 * Broadcasts a practice test change signal locally, via BroadcastChannel (same-origin tabs),
 * and via Firestore practice_tests_sync collection (cross-device real-time sync).
 */
export async function notifyPracticeTestRealtimeSync(details?: any): Promise<void> {
  questionSessionCache.clear();
  inFlightQuestionRequests.clear();

  // 1. Dispatch local event immediately
  if (typeof window !== "undefined") {
    window.dispatchEvent(new CustomEvent("practice-tests-updated"));
  }

  // 2. BroadcastChannel for instant same-browser multi-tab synchronization
  try {
    if (typeof window !== "undefined" && "BroadcastChannel" in window) {
      const bc = new BroadcastChannel("tuition_practice_tests_channel");
      bc.postMessage({ type: "PRACTICE_TESTS_UPDATED", timestamp: Date.now(), ...details });
      bc.close();
    }
  } catch (err) {}

  // 3. Firestore realtime signal for cross-device real-time synchronization
  try {
    const db = await getFirebaseDb();
    if (db) {
      const syncDocRef = doc(db, "practice_tests_sync", "latest");
      await setDoc(syncDocRef, {
        updatedAt: new Date().toISOString(),
        timestamp: Date.now(),
        ...details,
      }, { merge: true });
    }
  } catch (err) {
    console.warn("[PracticeTestService] Failed to send Firestore practice test sync signal:", err);
  }
}

async function openPracticeTestsDB(): Promise<IDBDatabase | null> {
  if (typeof window === "undefined" || !window.indexedDB) return null;
  return new Promise((resolve, reject) => {
    const request = window.indexedDB.open(IDB_DB_NAME, IDB_DB_VERSION);

    request.onupgradeneeded = () => {
      const db = request.result;
      if (!db.objectStoreNames.contains(IDB_SYNC_QUEUE_STORE)) {
        db.createObjectStore(IDB_SYNC_QUEUE_STORE, { keyPath: "id" });
      }
    };

    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error);
    request.onblocked = () => {
      console.warn("[PracticeTestService] IndexedDB open blocked by another tab.");
    };
  });
}

async function filterSyncQueue(predicate: (item: SyncQueueItem) => boolean): Promise<void> {
  const queue = await getSyncQueue();
  const filtered = queue.filter(predicate);
  await saveSyncQueue(filtered);
}

async function removeSyncQueueItemsForTopic(
  classGrade: string,
  subject: string,
  chapterNo: number,
  topicName: string
): Promise<void> {
  await filterSyncQueue((item) => {
    if (!item.context) return true;
    return !(
      item.context.classGrade === classGrade &&
      item.context.subject === subject &&
      item.context.chapterNo === chapterNo &&
      item.context.topicName === topicName
    );
  });
}

async function removeSyncQueueItemsForQuestion(questionId: string): Promise<void> {
  await filterSyncQueue((item) => {
    if (item.data && item.data.id) {
      return item.data.id !== questionId;
    }
    return true;
  });
}

async function queueOfflineDeleteTopic(context: {
  classGrade: string;
  subject: string;
  chapterNo: number;
  chapterName: string;
  topicName: string;
}): Promise<void> {
  await removeSyncQueueItemsForTopic(context.classGrade, context.subject, context.chapterNo, context.topicName);
  await addToSyncQueue({ action: "delete_topic", context });
}

async function queueOfflineDeleteQuestion(questionId: string): Promise<void> {
  await removeSyncQueueItemsForQuestion(questionId);
  await addToSyncQueue({ action: "delete_question", data: { id: questionId } });
}

export function initPracticeTestsRealtimeSync(): void {
  if (typeof window === "undefined") return;
  if (isRealtimeInitialized) return;
  isRealtimeInitialized = true;

  // A. Supabase Realtime Channel
  try {
    const supabaseAny = supabase as any;
    if (typeof supabaseAny.channel === "function" && !practiceTestsRealtimeChannel) {
      practiceTestsRealtimeChannel = supabaseAny
        .channel("practice_tests_changes")
        .on(
          "postgres_changes",
          { event: "*", schema: "public", table: "topic_assessment_questions" },
          async (payload: any) => {
            console.log("[PracticeTestService] Realtime event for topic assessment questions:", payload);
            try {
              await fetchAllPracticeTestsFromSupabase();
              if (typeof window !== "undefined") {
                window.dispatchEvent(new CustomEvent("practice-tests-updated"));
              }
            } catch (err) {
              console.warn("[PracticeTestService] Realtime refresh failed:", err);
            }
          }
        )
        .subscribe();
    }
  } catch (err) {
    console.warn("[PracticeTestService] Failed to initialize Supabase realtime sync:", err);
  }

  // B. BroadcastChannel for same-origin multi-tab sync
  try {
    if ("BroadcastChannel" in window) {
      const bc = new BroadcastChannel("tuition_practice_tests_channel");
      bc.onmessage = async (event) => {
        if (event.data?.type === "PRACTICE_TESTS_UPDATED") {
          console.log("[PracticeTestService] BroadcastChannel practice test update received");
          await fetchAllPracticeTestsFromSupabase();
          if (typeof window !== "undefined") {
            window.dispatchEvent(new CustomEvent("practice-tests-updated"));
          }
        }
      };
    }
  } catch (err) {}

  // C. Firestore Realtime Snapshot for cross-device sync
  getFirebaseDb().then((db) => {
    if (!db) return;
    try {
      const syncDocRef = doc(db, "practice_tests_sync", "latest");
      let lastProcessedTs = 0;

      onSnapshot(
        syncDocRef,
        async (snap) => {
          if (snap.exists()) {
            const data = snap.data();
            const ts = Number(data?.timestamp) || 0;
            if (ts && ts > lastProcessedTs) {
              lastProcessedTs = ts;
              console.log("[PracticeTestService] Firestore realtime practice test sync signal received:", data);
              await fetchAllPracticeTestsFromSupabase();
              if (typeof window !== "undefined") {
                window.dispatchEvent(new CustomEvent("practice-tests-updated"));
              }
            }
          }
        },
        (err) => {
          console.warn("[PracticeTestService] Firestore practice_tests_sync snapshot error:", err);
        }
      );
    } catch (err) {
      console.warn("[PracticeTestService] Failed setting up Firestore practice_tests_sync listener:", err);
    }
  });
}

if (typeof window !== "undefined") {
  initPracticeTestsRealtimeSync();
}

async function readSyncQueueFromIDB(): Promise<SyncQueueItem[]> {
  const db = await openPracticeTestsDB();
  if (!db) return memorySyncQueue;
  return new Promise((resolve, reject) => {
    const tx = db.transaction(IDB_SYNC_QUEUE_STORE, "readonly");
    const store = tx.objectStore(IDB_SYNC_QUEUE_STORE);
    const request = store.getAll();

    request.onsuccess = () => resolve(request.result as SyncQueueItem[]);
    request.onerror = () => reject(request.error);
  });
}

async function writeSyncQueueToIDB(queue: SyncQueueItem[]): Promise<void> {
  const db = await openPracticeTestsDB();
  if (!db) {
    memorySyncQueue = queue;
    return;
  }

  return new Promise((resolve, reject) => {
    const tx = db.transaction(IDB_SYNC_QUEUE_STORE, "readwrite");
    const store = tx.objectStore(IDB_SYNC_QUEUE_STORE);
    const clearRequest = store.clear();

    clearRequest.onsuccess = () => {
      for (const item of queue) {
        store.put(item);
      }
    };

    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error || new Error("IndexedDB transaction failed"));
    tx.onabort = () => reject(tx.error || new Error("IndexedDB transaction aborted"));
  });
}

export interface SaveTopicResult {
  success: boolean;
  count: number;
  message: string;
  error?: string;
  fromCache?: boolean;
}

export interface SyncQueueItem {
  id: string;
  action: "save_topic" | "delete_topic" | "delete_question" | "update_question";
  context?: {
    classGrade: string;
    subject: string;
    chapterNo: number;
    chapterName: string;
    topicName: string;
    rawText?: string;
  };
  data?: any;
  timestamp: number;
  retryCount?: number;
}

/**
 * Normalizes test ID for topic practice tests
 */
export function buildTopicTestId(
  classGrade: string,
  subject: string,
  chapterNo: number,
  topicName: string
): string {
  const normClass = String(classGrade || "").toLowerCase().trim().replace(/\s+/g, "_");
  const normSubj = String(subject || "").toLowerCase().trim().replace(/\s+/g, "_");
  const normTopic = String(topicName || "").toLowerCase().trim().replace(/[^a-z0-9]/g, "_");
  return `${normClass}__${normSubj}__ch${chapterNo}__${normTopic}`;
}

/**
 * Strict exact match comparison for topics to prevent deleting/overwriting unrelated tests
 */
export function isSubjectCompatible(subj1: string, subj2: string): boolean {
  const s1 = String(subj1 || "").toLowerCase().replace(/[^a-z0-9]/g, "");
  const s2 = String(subj2 || "").toLowerCase().replace(/[^a-z0-9]/g, "");
  if (!s1 || !s2) return true;
  if (s1 === s2) return true;
  if (s1.includes(s2) || s2.includes(s1)) return true;

  // Social Science sub-subject aliases
  const sstAliases = ["socialscience", "sst", "geography", "history", "politicalscience", "civics", "economics", "indianheritageandculture"];
  if (sstAliases.includes(s1) && sstAliases.includes(s2)) return true;

  return false;
}

/**
 * Strict exact match comparison for topics to prevent deleting/overwriting unrelated tests
 */
export function isExactTopicMatch(
  classGrade1: string,
  subject1: string,
  chapterNo1: number | string,
  topicName1: string,
  classGrade2: string,
  subject2: string,
  chapterNo2: number | string,
  topicName2: string
): boolean {
  if (Number(chapterNo1) !== Number(chapterNo2)) return false;

  const c1 = String(classGrade1 || "").toLowerCase().replace(/[^a-z0-9]/g, "");
  const c2 = String(classGrade2 || "").toLowerCase().replace(/[^a-z0-9]/g, "");
  if (c1 && c2 && c1 !== c2 && !c1.includes(c2) && !c2.includes(c1)) return false;

  if (!isSubjectCompatible(subject1, subject2)) return false;

  const t1 = String(topicName1 || "").toLowerCase().trim();
  const t2 = String(topicName2 || "").toLowerCase().trim();
  if (!t1 || !t2) return false;
  if (t1 === t2) return true;

  const ct1 = t1.replace(/[^a-z0-9]/g, "");
  const ct2 = t2.replace(/[^a-z0-9]/g, "");
  if (ct1 && ct2 && ct1 === ct2) return true;

  // Extract topic numbers e.g. "Topic 12 : ..." -> 12
  const num1Match = t1.match(/(?:topic|part|pt|ch)?\s*(\d+)/i);
  const num2Match = t2.match(/(?:topic|part|pt|ch)?\s*(\d+)/i);

  if (num1Match && num2Match && num1Match[1] === num2Match[1]) {
    const text1 = t1.replace(/^(?:topic|part|pt|ch)?\s*\d*\s*[:\–\-]?\s*/i, "").trim().replace(/[^a-z0-9]/g, "");
    const text2 = t2.replace(/^(?:topic|part|pt|ch)?\s*\d*\s*[:\–\-]?\s*/i, "").trim().replace(/[^a-z0-9]/g, "");
    if (!text1 || !text2) return true;
    if (text1 === text2 || text1.includes(text2) || text2.includes(text1)) return true;
  }

  const text1Only = t1.replace(/^(?:topic|part|pt|ch)?\s*\d*\s*[:\–\-]?\s*/i, "").trim().replace(/[^a-z0-9]/g, "");
  const text2Only = t2.replace(/^(?:topic|part|pt|ch)?\s*\d*\s*[:\–\-]?\s*/i, "").trim().replace(/[^a-z0-9]/g, "");
  if (text1Only && text2Only && text1Only.length > 3 && text2Only.length > 3) {
    if (text1Only === text2Only || text1Only.includes(text2Only) || text2Only.includes(text1Only)) return true;
  }

  return false;
}

export async function resolveQuestionImageUrls(
  questions: ParsedAssessmentQuestion[]
): Promise<ParsedAssessmentQuestion[]> {
  if (!Array.isArray(questions)) return [];
  return Promise.all(
    questions.map(async (q) => {
      if (q.imageUrl && typeof q.imageUrl === "string") {
        try {
          const viewUrl = await getResolvedViewUrl("academy-connect-files", q.imageUrl);
          return { ...q, imageUrl: viewUrl };
        } catch (e) {
          return q;
        }
      }
      return q;
    })
  );
}

/**
 * Normalizes question ID
 */
export function buildQuestionId(
  classGrade: string,
  subject: string,
  chapterNo: number,
  topicName: string,
  index: number
): string {
  const base = buildTopicTestId(classGrade, subject, chapterNo, topicName);
  return `q_${base}_${index + 1}_${Math.random().toString(36).substring(2, 7)}`;
}

// ----------------------------------------------------
// LOCAL CACHE & STORAGE SYNC HELPERS
// ----------------------------------------------------

const PRACTICE_TESTS_BUCKET = "academy-connect-files";
const PRACTICE_TESTS_FILE_PATH = "practice_tests/test_bank.json";
const PRACTICE_TEST_ATTEMPTS_FILE_PATH = "practice_tests/test_attempts.json";

export interface TopicPracticeTestMetadata {
  id: string;
  classGrade: string;
  subject: string;
  chapterNo: number;
  chapterName: string;
  topicName: string;
  questionCount: number;
  lastUpdated: string;
}

// In-Memory RAM cache for active session fast access
let memoryTestBank: Record<string, TopicPracticeTest> = {};

export interface ScoreButtonStyles {
  container: string;
  icon: string;
  scoreText: string;
  labelText: string;
}

export function getScoreButtonStyles(isAttempted: boolean, percentage?: number | null): ScoreButtonStyles {
  if (!isAttempted || percentage === undefined || percentage === null || isNaN(percentage)) {
    // Normal green Test button
    return {
      container: "bg-emerald-50 dark:bg-emerald-950/40 border-emerald-200/80 dark:border-emerald-800/60 hover:bg-emerald-100 dark:hover:bg-emerald-900/60 text-emerald-800 dark:text-emerald-200",
      icon: "text-emerald-600 dark:text-emerald-400",
      scoreText: "text-emerald-800 dark:text-emerald-200",
      labelText: "text-emerald-600 dark:text-emerald-400",
    };
  }

  const pct = Math.round(percentage);

  if (pct >= 90) {
    // Green (90–100%)
    return {
      container: "bg-emerald-50 dark:bg-emerald-950/40 border-emerald-300 dark:border-emerald-800 hover:bg-emerald-100 dark:hover:bg-emerald-900/60 text-emerald-800 dark:text-emerald-200",
      icon: "text-emerald-600 dark:text-emerald-400",
      scoreText: "text-emerald-800 dark:text-emerald-200",
      labelText: "text-emerald-600 dark:text-emerald-400",
    };
  } else if (pct >= 75) {
    // Blue (75–89%)
    return {
      container: "bg-blue-50 dark:bg-blue-950/40 border-blue-300 dark:border-blue-800 hover:bg-blue-100 dark:hover:bg-blue-900/60 text-blue-800 dark:text-blue-200",
      icon: "text-blue-600 dark:text-blue-400",
      scoreText: "text-blue-800 dark:text-blue-200",
      labelText: "text-blue-600 dark:text-blue-400",
    };
  } else if (pct >= 50) {
    // Orange (50–74%)
    return {
      container: "bg-amber-50 dark:bg-amber-950/40 border-amber-300 dark:border-amber-800 hover:bg-amber-100 dark:hover:bg-amber-900/60 text-amber-800 dark:text-amber-200",
      icon: "text-amber-600 dark:text-amber-400",
      scoreText: "text-amber-800 dark:text-amber-200",
      labelText: "text-amber-600 dark:text-amber-400",
    };
  } else {
    // Red (Below 50%)
    return {
      container: "bg-rose-50 dark:bg-rose-950/40 border-rose-300 dark:border-rose-800 hover:bg-rose-100 dark:hover:bg-rose-900/60 text-rose-800 dark:text-rose-200",
      icon: "text-rose-600 dark:text-rose-400",
      scoreText: "text-rose-800 dark:text-rose-200",
      labelText: "text-rose-600 dark:text-rose-400",
    };
  }
}

export async function purgeAllPracticeTestsData(): Promise<void> {
  memoryTestBank = {};
  if (typeof window !== "undefined") {
    try {
      localStorage.removeItem(TESTS_CACHE_KEY);
      localStorage.removeItem(SYNC_QUEUE_KEY);
      await writeSyncQueueToIDB([]);
    } catch (err) {
      console.warn("[PracticeTestService] Error clearing local cache:", err);
    }
  }

  try {
    const jsonString = JSON.stringify({}, null, 2);
    const blob = new Blob([jsonString], { type: "application/json" });
    await supabase.storage
      .from(PRACTICE_TESTS_BUCKET)
      .upload(PRACTICE_TESTS_FILE_PATH, blob, { upsert: true });
  } catch (err) {
    console.warn("[PracticeTestService] Storage purge warning:", err);
  }

  try {
    await supabase
      .from("topic_assessment_questions")
      .delete()
      .neq("id", "00000000-0000-0000-0000-000000000000");
  } catch (err) {
    console.warn("[PracticeTestService] DB purge warning:", err);
  }

  if (typeof window !== "undefined") {
    window.dispatchEvent(new CustomEvent("practice-tests-updated"));
  }
}

export async function syncTestBankToSupabaseStorage(bank: Record<string, TopicPracticeTest>): Promise<boolean> {
  try {
    const jsonString = JSON.stringify(bank, null, 2);
    const blob = new Blob([jsonString], { type: "application/json" });
    const { error } = await supabase.storage
      .from(PRACTICE_TESTS_BUCKET)
      .upload(PRACTICE_TESTS_FILE_PATH, blob, { upsert: true });
    if (error) {
      console.warn("[PracticeTestService] Storage sync warning:", error.message || error);
      return false;
    }
    return true;
  } catch (err) {
    console.warn("[PracticeTestService] Storage sync exception:", err);
    return false;
  }
}

export async function fetchTestBankFromSupabaseStorage(): Promise<Record<string, TopicPracticeTest> | null> {
  try {
    const { data, error } = await supabase.storage
      .from(PRACTICE_TESTS_BUCKET)
      .download(PRACTICE_TESTS_FILE_PATH);
    if (!error && data) {
      const text = await data.text();
      if (text) {
        const parsed = JSON.parse(text);
        if (parsed && typeof parsed === "object") {
          return parsed;
        }
      }
    }
  } catch (err) {
    console.warn("[PracticeTestService] Storage fetch error:", err);
  }
  return null;
}

export async function syncTestAttemptsToSupabaseStorage(attempts: TestAttemptRecord[]): Promise<boolean> {
  try {
    const jsonString = JSON.stringify(attempts, null, 2);
    const blob = new Blob([jsonString], { type: "application/json" });
    const { error } = await supabase.storage
      .from(PRACTICE_TESTS_BUCKET)
      .upload(PRACTICE_TEST_ATTEMPTS_FILE_PATH, blob, { upsert: true });
    if (error) {
      console.warn("[PracticeTestService] Storage attempts sync warning:", error.message || error);
      return false;
    }
    return true;
  } catch (err) {
    console.warn("[PracticeTestService] Storage attempts sync exception:", err);
    return false;
  }
}

export async function fetchTestAttemptsFromSupabaseStorage(): Promise<TestAttemptRecord[] | null> {
  try {
    const { data, error } = await supabase.storage
      .from(PRACTICE_TESTS_BUCKET)
      .download(PRACTICE_TEST_ATTEMPTS_FILE_PATH);
    if (!error && data) {
      const text = await data.text();
      if (text) {
        const parsed = JSON.parse(text);
        if (Array.isArray(parsed)) {
          return parsed;
        }
      }
    }
  } catch (err) {
    console.warn("[PracticeTestService] Storage attempts fetch error:", err);
  }
  return null;
}

export function getLocalTestBank(): Record<string, TopicPracticeTest> {
  return memoryTestBank;
}

export function getLocalTopicMetadata(): Record<string, TopicPracticeTestMetadata> {
  if (typeof window === "undefined") return {};
  try {
    const raw = safeLocalStorageGetItem(TESTS_CACHE_KEY);
    return raw ? JSON.parse(raw) : {};
  } catch (err) {
    return {};
  }
}

export function saveLocalTestBank(bank: Record<string, TopicPracticeTest>, options?: { silent?: boolean }): void {
  memoryTestBank = { ...bank };

  if (typeof window === "undefined") return;
  try {
    const metadataMap: Record<string, TopicPracticeTestMetadata> = {};
    for (const key of Object.keys(bank)) {
      const test = bank[key];
      if (!test) continue;
      metadataMap[key] = {
        id: test.id,
        classGrade: test.classGrade || "",
        subject: test.subject || "",
        chapterNo: Number(test.chapterNo) || 1,
        chapterName: test.chapterName || "",
        topicName: test.topicName || "",
        questionCount: Array.isArray(test.questions) ? test.questions.length : 0,
        lastUpdated: test.updatedAt || new Date().toISOString(),
      };
    }

    const entries = Object.entries(metadataMap).sort(([, a], [, b]) =>
      new Date(b.lastUpdated).getTime() - new Date(a.lastUpdated).getTime()
    );
    let trimmedMap: Record<string, TopicPracticeTestMetadata> = {};
    let json = "";

    for (const [key, metadata] of entries) {
      trimmedMap[key] = metadata;
      json = JSON.stringify(trimmedMap);
      if (json.length * 2 > MAX_LOCAL_STORAGE_ITEM_BYTES) {
        delete trimmedMap[key];
        break;
      }
    }

    safeLocalStorageSetItem(TESTS_CACHE_KEY, JSON.stringify(trimmedMap));
  } catch (err: any) {
    console.warn("[PracticeTestService] Error saving metadata:", err);
  } finally {
    if (!options?.silent) {
      window.dispatchEvent(new CustomEvent("practice-tests-updated"));
    }
  }
}

export function updateLocalTopicCache(test: TopicPracticeTest): void {
  memoryTestBank[test.id] = test;
  saveLocalTestBank(memoryTestBank);
  syncTestBankToSupabaseStorage(memoryTestBank).catch(() => {});
}

export function removeLocalTopicCache(testId: string): void {
  delete memoryTestBank[testId];

  // Also delete by normalized key match
  const parts = testId.split("__");
  if (parts.length >= 4) {
    const classGrade = parts[0];
    const subject = parts[1];
    const chapterNoStr = parts[2];
    const normTopic = parts.slice(3).join("__").toLowerCase().replace(/[^a-z0-9]/g, "");

    Object.keys(memoryTestBank).forEach((key) => {
      const t = memoryTestBank[key];
      if (
        t &&
        `ch${t.chapterNo}` === chapterNoStr &&
        (t.classGrade || "").toLowerCase().replace(/[^a-z0-9]/g, "") === classGrade.toLowerCase().replace(/[^a-z0-9]/g, "") &&
        (t.subject || "").toLowerCase().replace(/[^a-z0-9]/g, "") === subject.toLowerCase().replace(/[^a-z0-9]/g, "") &&
        (t.topicName || "").toLowerCase().replace(/[^a-z0-9]/g, "") === normTopic
      ) {
        delete memoryTestBank[key];
      }
    });
  }
  saveLocalTestBank(memoryTestBank);
}

// ----------------------------------------------------
// SYNC QUEUE HELPERS (OFFLINE SUPPORT)
// ----------------------------------------------------

export async function getSyncQueue(): Promise<SyncQueueItem[]> {
  if (typeof window === "undefined") return [];
  try {
    return await readSyncQueueFromIDB();
  } catch (err) {
    console.warn("[PracticeTestService] Error reading sync queue from IndexedDB:", err);
    return memorySyncQueue;
  }
}

export async function saveSyncQueue(queue: SyncQueueItem[]): Promise<void> {
  if (typeof window === "undefined") return;
  try {
    const cleanQueue = queue.map((item) => {
      if (item.context && (item.context as any).rawText) {
        const { rawText, ...restContext } = item.context as any;
        return { ...item, context: restContext };
      }
      return item;
    });
    memorySyncQueue = cleanQueue;
    await writeSyncQueueToIDB(cleanQueue);
  } catch (err) {
    console.warn("[PracticeTestService] Error saving sync queue:", err);
    memorySyncQueue = queue;
  }
}

export async function addToSyncQueue(item: Omit<SyncQueueItem, "id" | "timestamp" | "retryCount">): Promise<void> {
  const queue = await getSyncQueue();

  const cleanQueue = queue.filter((q) => {
    if (q.action === item.action && q.context && item.context) {
      return (
        q.context.classGrade !== item.context.classGrade ||
        q.context.subject !== item.context.subject ||
        q.context.chapterNo !== item.context.chapterNo ||
        q.context.topicName !== item.context.topicName
      );
    }
    return true;
  });

  cleanQueue.push({
    ...item,
    id: `queue_${Date.now()}_${Math.random().toString(36).substring(2, 6)}`,
    timestamp: Date.now(),
    retryCount: 0,
  });

  await saveSyncQueue(cleanQueue);
}

/**
 * Automatically sync queued offline changes to Supabase when online.
 */
export async function processSyncQueue(): Promise<{ synced: number; failed: number }> {
  const queue = await getSyncQueue();
  if (queue.length === 0) return { synced: 0, failed: 0 };

  if (typeof navigator !== "undefined" && !navigator.onLine) {
    return { synced: 0, failed: queue.length };
  }

  console.log(`[PracticeTestService] Processing ${queue.length} offline sync items...`);
  let synced = 0;
  let failed = 0;
  const remaining: SyncQueueItem[] = [];

  for (const item of queue) {
    if (item.retryCount !== undefined && item.retryCount >= MAX_SYNC_RETRIES) {
      console.warn(
        `[PracticeTestService] Dropping sync queue item after ${item.retryCount} failed attempts:`,
        item
      );
      continue;
    }

    try {
      let itemSuccess = false;
      if (item.action === "save_topic" && item.context && item.data) {
        const res = await pushTopicToSupabase(item.context, item.data, item.context.rawText || "");
        itemSuccess = res.success;
      } else if (item.action === "delete_topic" && item.context) {
        const res = await deleteTopicFromSupabase(
          item.context.classGrade,
          item.context.subject,
          item.context.chapterNo,
          item.context.topicName
        );
        itemSuccess = res.success;
      } else if (item.action === "delete_question" && item.data?.id) {
        const res = await deleteAssessmentQuestion(item.data.id);
        itemSuccess = res.success;
      } else if (item.action === "update_question" && item.data?.id && item.data?.updates) {
        const res = await updateAssessmentQuestion(item.data.id, item.data.updates);
        itemSuccess = res.success;
      } else {
        itemSuccess = true;
      }

      if (itemSuccess) {
        synced++;
      } else {
        const queuedItem = { ...item, retryCount: (item.retryCount || 0) + 1 };
        if (queuedItem.retryCount < MAX_SYNC_RETRIES) {
          remaining.push(queuedItem);
        } else {
          failed++;
          console.warn(`[PracticeTestService] Sync queue item reached retry limit and will be dropped:`, queuedItem);
        }
      }
    } catch (err) {
      console.warn("[PracticeTestService] Failed syncing item:", item, err);
      const queuedItem = { ...item, retryCount: (item.retryCount || 0) + 1 };
      if (queuedItem.retryCount < MAX_SYNC_RETRIES) {
        remaining.push(queuedItem);
      } else {
        failed++;
        console.warn(`[PracticeTestService] Sync queue item reached retry limit due to exception and will be dropped:`, queuedItem);
      }
    }
  }

  await saveSyncQueue(remaining);

  if (synced > 0 && typeof window !== "undefined") {
    window.dispatchEvent(
      new CustomEvent("practice-tests-synced", {
        detail: { message: "Sync completed successfully.", count: synced },
      })
    );
  }

  return { synced, failed };
}

// Auto-listen to online event
if (typeof window !== "undefined") {
  window.addEventListener("online", () => {
    processSyncQueue().catch((err) => console.warn("Error processing sync queue:", err));
  });
}

// ----------------------------------------------------
// SUPABASE DATABASE CONVERTERS
// ----------------------------------------------------

function generateDeterministicUuid(seedStr: string): string {
  let hash1 = 0, hash2 = 0, hash3 = 0, hash4 = 0;
  for (let i = 0; i < seedStr.length; i++) {
    const code = seedStr.charCodeAt(i);
    hash1 = (hash1 * 31 + code) & 0x7fffffff;
    hash2 = (hash2 * 33 + code) & 0x7fffffff;
    hash3 = (hash3 * 37 + code) & 0x7fffffff;
    hash4 = (hash4 * 39 + code) & 0x7fffffff;
  }
  const hex1 = hash1.toString(16).padStart(8, "0");
  const hex2 = hash2.toString(16).padStart(4, "0").slice(0, 4);
  const hex3 = hash3.toString(16).padStart(4, "0").slice(0, 4);
  const hex4 = hash4.toString(16).padStart(4, "0").slice(0, 4);
  const hex5 = (hash1 ^ hash2 ^ hash3).toString(16).padStart(12, "0").slice(0, 12);

  return `${hex1}-${hex2}-4${hex3.slice(1)}-8${hex4.slice(1)}-${hex5}`;
}

function toSupabaseRow(
  q: ParsedAssessmentQuestion,
  context: { classGrade: string; subject: string; chapterNo: number; chapterName: string; topicName: string },
  rawText: string,
  idx: number
) {
  let validId = q.id;
  if (!validId || !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(validId)) {
    const seed = `${context.classGrade}__${context.subject}__ch${context.chapterNo}__${context.topicName}__q${idx + 1}`;
    validId = generateDeterministicUuid(seed);
  }

  // Preserve image fields inside raw_text JSON tag if present so they persist across reloads
  let metaRawText = rawText || q.rawText || "";
  if (q.imageUrl) {
    try {
      const imageMeta = JSON.stringify({
        imageUrl: q.imageUrl,
        imageLabel: q.imageLabel,
        imagePosition: q.imagePosition,
      });
      if (!metaRawText.includes("[IMG_META:")) {
        metaRawText += `\n[IMG_META:${imageMeta}]`;
      }
    } catch {}
  }

  const row: any = {
    id: validId,
    class_id: String(context.classGrade || "").trim(),
    subject_id: String(context.subject || "").trim(),
    chapter_id: String(context.chapterNo || ""),
    topic_id: String(context.topicName || "").trim(),
    question_type: q.type === "mcq" ? "MCQ" : "TRUE_FALSE",
    question: String(q.question || "").trim(),
    options: q.options || [],
    correct_answer: String(q.correctAnswer || "").trim(),
    published: q.published !== false,
    order_index: idx + 1,
    raw_text: metaRawText,
    created_at: q.createdAt || new Date().toISOString(),
    updated_at: new Date().toISOString(),
  };

  return row;
}

function fromSupabaseRow(row: any, fallbackChapterName: string = ""): ParsedAssessmentQuestion {
  let optionsList: string[] = [];
  if (Array.isArray(row.options)) {
    optionsList = row.options;
  } else if (typeof row.options === "string") {
    try {
      optionsList = JSON.parse(row.options);
    } catch {
      optionsList = [row.options];
    }
  }

  const rawType = String(row.question_type || "").toLowerCase();
  const qType: "mcq" | "true_false" = rawType.includes("mcq") ? "mcq" : "true_false";

  let imageUrl = row.image_url || row.imageUrl || undefined;
  let imageLabel = row.image_label || row.imageLabel || undefined;
  let imagePosition: "above" | "below" = (row.image_position || row.imagePosition || "below").toLowerCase() === "above" ? "above" : "below";

  if (!imageUrl && row.raw_text && typeof row.raw_text === "string" && row.raw_text.includes("[IMG_META:")) {
    try {
      const match = row.raw_text.match(/\[IMG_META:(.*?)\]/s);
      if (match && match[1]) {
        const meta = JSON.parse(match[1]);
        if (meta.imageUrl) imageUrl = meta.imageUrl;
        if (meta.imageLabel) imageLabel = meta.imageLabel;
        if (meta.imagePosition === "above") imagePosition = "above";
      }
    } catch {}
  }

  return {
    id: String(row.id),
    classGrade: String(row.class_id || ""),
    subject: String(row.subject_id || ""),
    chapterNo: Number(row.chapter_id) || 1,
    chapterName: fallbackChapterName || `Chapter ${row.chapter_id}`,
    topicName: String(row.topic_id || ""),
    type: qType,
    question: String(row.question || ""),
    options: optionsList,
    correctAnswer: String(row.correct_answer || ""),
    imageUrl,
    imageLabel,
    imagePosition,
    published: row.published !== false,
    orderIndex: Number(row.order_index) || 0,
    rawText: row.raw_text || "",
    createdAt: row.created_at || new Date().toISOString(),
    updatedAt: row.updated_at || new Date().toISOString(),
  };
}

// ----------------------------------------------------
// CORE SERVICE API (SINGLE SOURCE OF TRUTH)
// ----------------------------------------------------

/**
 * Pushes topic assessment questions to Supabase table `topic_assessment_questions`.
 */
async function pushTopicToSupabase(
  context: { classGrade: string; subject: string; chapterNo: number; chapterName: string; topicName: string },
  questions: ParsedAssessmentQuestion[],
  rawText: string
): Promise<{ success: boolean; error?: string }> {
  try {
    console.log(
      `[PracticeTestService] Pushing ${questions.length} questions to Supabase DB for Class: "${context.classGrade}", Subj: "${context.subject}", Ch: ${context.chapterNo}, Topic: "${context.topicName}"`
    );

    // 1. Prepare rows with valid UUID IDs
    const rows = questions.map((q, idx) => toSupabaseRow(q, context, rawText, idx));
    const newQuestionIds = new Set(rows.map((r) => r.id));

    // Clean up obsolete/orphaned question rows in Supabase for this topic
    try {
      const { data: existingRows } = await supabase
        .from("topic_assessment_questions")
        .select("id, class_id, subject_id, chapter_id, topic_id")
        .eq("chapter_id", String(context.chapterNo));

      if (Array.isArray(existingRows)) {
        const orphanIds = existingRows
          .filter(
            (row) =>
              isExactTopicMatch(
                context.classGrade,
                context.subject,
                context.chapterNo,
                context.topicName,
                row.class_id || "",
                row.subject_id || "",
                row.chapter_id || "",
                row.topic_id || ""
              ) && !newQuestionIds.has(String(row.id))
          )
          .map((row) => String(row.id));

        if (orphanIds.length > 0) {
          console.log(`[PracticeTestService] Deleting ${orphanIds.length} obsolete question rows from Supabase.`);
          await supabase.from("topic_assessment_questions").delete().in("id", orphanIds);
        }
      }
    } catch (e) {
      console.warn("[PracticeTestService] Error cleaning up orphaned rows:", e);
    }

    // 2. Try upsert by ID
    let { error: insertErr } = await supabase
      .from("topic_assessment_questions")
      .upsert(rows, { onConflict: "id" });

    if (insertErr) {
      console.warn("[PracticeTestService] Primary upsert failed, retrying insert without optional fields...", insertErr.message);
      const rowsEssential = rows.map(({ image_position, image_label, raw_text, ...rest }: any) => rest);
      let { error: retry1 } = await supabase
        .from("topic_assessment_questions")
        .upsert(rowsEssential, { onConflict: "id" });

      if (!retry1) {
        insertErr = null;
      } else {
        // Plain insert fallback
        let { error: retry2 } = await supabase
          .from("topic_assessment_questions")
          .insert(rowsEssential);
        if (!retry2) {
          insertErr = null;
        }
      }
    }

    if (insertErr) {
      console.warn("[PracticeTestService] Supabase insert warning:", insertErr.message || insertErr);
      return { success: false, error: insertErr.message || JSON.stringify(insertErr) };
    }

    console.log(`[PracticeTestService] Successfully persisted ${questions.length} questions to Supabase DB.`);
    return { success: true };
  } catch (err: any) {
    console.warn("[PracticeTestService] Exception in pushTopicToSupabase:", err);
    return { success: false, error: err.message || "Network request failed" };
  }
}

/**
 * Delete topic assessment questions from Supabase
 */
async function deleteTopicFromSupabase(
  classGrade: string,
  subject: string,
  chapterNo: number,
  topicName: string
): Promise<{ success: boolean; message?: string; error?: string }> {
  try {
    const classTrim = (classGrade || "").trim();
    const subjTrim = (subject || "").trim();
    const topicTrim = (topicName || "").trim();
    const chStr = String(chapterNo);

    console.log("[PracticeTestService] Deleting topic test from Supabase table public.topic_assessment_questions:", {
      class_id: classTrim,
      subject_id: subjTrim,
      chapter_id: chStr,
      topic_id: topicTrim,
    });

    const idsToDelete = new Set<string>();

    // 1. Query rows in Supabase to find all matching questions by chapter & exact topic match
    try {
      const { data: dbRows, error: selectErr } = await supabase
        .from("topic_assessment_questions")
        .select("id, class_id, subject_id, chapter_id, topic_id")
        .range(0, 9999);

      if (!selectErr && Array.isArray(dbRows)) {
        dbRows.forEach((row) => {
          if (
            isExactTopicMatch(
              classGrade,
              subject,
              chapterNo,
              topicName,
              row.class_id || "",
              row.subject_id || "",
              row.chapter_id || "",
              row.topic_id || ""
            ) ||
            (String(row.chapter_id) === chStr && String(row.topic_id || "").trim().toLowerCase() === topicTrim.toLowerCase())
          ) {
            idsToDelete.add(String(row.id));
          }
        });
      }
    } catch (e) {
      console.warn("[PracticeTestService] Error querying rows for deletion:", e);
    }

    // 2. Check local bank for any question IDs associated with this test
    try {
      const bank = getLocalTestBank();
      const testId = buildTopicTestId(classGrade, subject, chapterNo, topicName);
      if (bank[testId]?.questions) {
        bank[testId].questions.forEach((q) => idsToDelete.add(q.id));
      }
      Object.keys(bank).forEach((k) => {
        const t = bank[k];
        if (
          t &&
          isExactTopicMatch(
            classGrade,
            subject,
            chapterNo,
            topicName,
            t.classGrade,
            t.subject,
            t.chapterNo,
            t.topicName
          )
        ) {
          (t.questions || []).forEach((q) => idsToDelete.add(q.id));
        }
      });
    } catch (e) {}

    // Delete matching rows by IDs in chunks of 100
    if (idsToDelete.size > 0) {
      const idArray = Array.from(idsToDelete);
      console.log(`[PracticeTestService] Deleting ${idArray.length} questions by ID from Supabase:`, idArray);
      for (let i = 0; i < idArray.length; i += 100) {
        const chunk = idArray.slice(i, i + 100);
        await supabase.from("topic_assessment_questions").delete().in("id", chunk);
      }
    }

    // Execute direct property match deletes for fallback safety
    await supabase.from("topic_assessment_questions").delete().match({
      class_id: classTrim,
      subject_id: subjTrim,
      chapter_id: chStr,
      topic_id: topicTrim,
    });

    await supabase.from("topic_assessment_questions").delete().match({
      chapter_id: chStr,
      topic_id: topicTrim,
    });

    await supabase.from("topic_assessment_questions").delete().eq("topic_id", topicName);

    // Verification step: ensure zero matching rows remain in Supabase DB
    const { data: remainingCheck } = await supabase
      .from("topic_assessment_questions")
      .select("id, class_id, subject_id, chapter_id, topic_id")
      .range(0, 9999);

    if (Array.isArray(remainingCheck) && remainingCheck.length > 0) {
      const stubbornIds = remainingCheck
        .filter((row) =>
          isExactTopicMatch(
            classGrade,
            subject,
            chapterNo,
            topicName,
            row.class_id || "",
            row.subject_id || "",
            row.chapter_id || "",
            row.topic_id || ""
          ) ||
          (String(row.chapter_id) === chStr && String(row.topic_id || "").trim().toLowerCase() === topicTrim.toLowerCase())
        )
        .map((r) => String(r.id));

      if (stubbornIds.length > 0) {
        console.warn(`[PracticeTestService] Cleaned up ${stubbornIds.length} remaining rows during deletion verification.`);
        await supabase.from("topic_assessment_questions").delete().in("id", stubbornIds);
      }
    }

    // Also delete all student attempts and scores for this topic from Supabase DB, Storage, and caches
    try {
      await deleteTopicAttemptsFromPersistence(classGrade, subject, chapterNo, topicName);
    } catch (attErr) {
      console.warn("[PracticeTestService] Error deleting student attempts during topic delete:", attErr);
    }

    await removeSyncQueueItemsForTopic(classGrade, subject, chapterNo, topicName);

    return { success: true, message: "Practice Test deleted successfully." };
  } catch (err: any) {
    console.error("[PracticeTestService] Exception in deleteTopicFromSupabase:", err);
    return { success: false, error: err.message, message: err.message };
  }
}

/**
 * Saves a Topic Practice Test with all its questions to Supabase and updates local cache.
 */
export async function saveTopicPracticeTest(
  context: {
    classGrade: string;
    subject: string;
    chapterNo: number;
    chapterName: string;
    topicName: string;
    rawText: string;
  },
  questions: ParsedAssessmentQuestion[]
): Promise<SaveTopicResult> {
  if (!questions || questions.length === 0) {
    return {
      success: false,
      count: 0,
      message: "Cannot save empty practice test. Please enter valid questions.",
      error: "No valid questions found.",
    };
  }

  const testId = buildTopicTestId(
    context.classGrade,
    context.subject,
    context.chapterNo,
    context.topicName
  );

  const formattedQuestions: ParsedAssessmentQuestion[] = questions.map((q, idx) => ({
    ...q,
    id: q.id || buildQuestionId(context.classGrade, context.subject, context.chapterNo, context.topicName, idx),
    classGrade: context.classGrade,
    subject: context.subject,
    chapterNo: context.chapterNo,
    chapterName: context.chapterName,
    topicName: context.topicName,
    published: q.published !== false,
    orderIndex: idx + 1,
    rawText: context.rawText,
    createdAt: q.createdAt || new Date().toISOString(),
    updatedAt: new Date().toISOString(),
  }));

  const topicTest: TopicPracticeTest = {
    id: testId,
    classGrade: context.classGrade,
    subject: context.subject,
    chapterNo: context.chapterNo,
    chapterName: context.chapterName,
    topicName: context.topicName,
    rawText: context.rawText,
    questions: formattedQuestions,
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    uploadedBy: "Admin",
  };

  // 1. Always update local cache instantly for zero latency
  updateLocalTopicCache(topicTest);
  questionSessionCache.clear();

  // 2. Push to Supabase
  const pushRes = await pushTopicToSupabase(context, formattedQuestions, context.rawText);

  // 3. Sync to Supabase Storage JSON backup
  const storageSynced = await syncTestBankToSupabaseStorage(getLocalTestBank()).catch(() => false);

  // 4. Dispatch update event
  await notifyPracticeTestRealtimeSync({ testId, action: "save_topic" });

  if (!pushRes.success) {
    if (!storageSynced) {
      await addToSyncQueue({
        action: "save_topic",
        context,
        data: formattedQuestions,
      });
    }

    console.error(`[PracticeTestService] Supabase save error: ${pushRes.error}`);
    return {
      success: false,
      count: 0,
      message: pushRes.error || "Failed to save Practice Test to Supabase.",
      error: pushRes.error || "Failed to save Practice Test to Supabase.",
    };
  }

  return {
    success: true,
    count: formattedQuestions.length,
    message: "Practice Test saved successfully.",
  };
}

// In-memory session cache and request deduplication map for practice tests
const questionSessionCache = new Map<string, TopicPracticeTest | null>();
const inFlightQuestionRequests = new Map<string, Promise<TopicPracticeTest | null>>();

/**
 * Retrieves a Topic Practice Test from Supabase (falling back to local cache if offline).
 * Uses session caching, column optimization, and promise deduplication for instant loading.
 */
export async function getTopicPracticeTest(
  classGrade: string,
  subject: string,
  chapterNo: number,
  topicName: string,
  options?: { publishedOnly?: boolean }
): Promise<TopicPracticeTest | null> {
  const testId = buildTopicTestId(classGrade, subject, chapterNo, topicName);
  const cacheKey = `${testId}__${options?.publishedOnly ? "published" : "all"}`;

  if (questionSessionCache.has(cacheKey)) {
    return questionSessionCache.get(cacheKey) || null;
  }

  if (inFlightQuestionRequests.has(cacheKey)) {
    return inFlightQuestionRequests.get(cacheKey)!;
  }

  const fetchPromise = (async () => {
    let cachedTest = getTopicPracticeTestSync(classGrade, subject, chapterNo, topicName, options);

    try {
      const { data, error } = await supabase
        .from("topic_assessment_questions")
        .select("*")
        .eq("chapter_id", String(chapterNo))
        .order("order_index", { ascending: true })
        .range(0, 9999);

      if (!error && Array.isArray(data)) {
        const matchingRows = data.filter((row) => {
          const isMatch = isExactTopicMatch(
            classGrade,
            subject,
            chapterNo,
            topicName,
            row.class_id || "",
            row.subject_id || "",
            row.chapter_id || "",
            row.topic_id || ""
          );
          if (!isMatch) return false;
          if (options?.publishedOnly) {
            return row.published !== false;
          }
          return true;
        });

        if (matchingRows.length > 0) {
          let parsedQuestions = matchingRows.map((row) => {
            const parsed = fromSupabaseRow(row, cachedTest?.chapterName || `Chapter ${chapterNo}`);
            if (!parsed.imageUrl && cachedTest?.questions) {
              const cachedQ = cachedTest.questions.find((cq) => cq.id === parsed.id);
              if (cachedQ && cachedQ.imageUrl) {
                parsed.imageUrl = cachedQ.imageUrl;
              }
            }
            return parsed;
          });

          parsedQuestions = await resolveQuestionImageUrls(parsedQuestions);

          const remoteTest: TopicPracticeTest = {
            id: testId,
            classGrade,
            subject,
            chapterNo,
            chapterName: cachedTest?.chapterName || `Chapter ${chapterNo}`,
            topicName,
            rawText: matchingRows[0]?.raw_text || cachedTest?.rawText || "",
            questions: parsedQuestions,
            createdAt: matchingRows[0]?.created_at || new Date().toISOString(),
            updatedAt: matchingRows[0]?.updated_at || new Date().toISOString(),
            uploadedBy: "Admin",
          };

          // Refresh local cache with latest database rows
          updateLocalTopicCache(remoteTest);
          questionSessionCache.set(cacheKey, remoteTest);
          return remoteTest;
        } else if (cachedTest && cachedTest.questions && cachedTest.questions.length > 0) {
          questionSessionCache.set(cacheKey, cachedTest);
          return cachedTest;
        } else {
          removeLocalTopicCache(testId);
          questionSessionCache.set(cacheKey, null);
          return null;
        }
      }
    } catch (err) {
      console.warn("[PracticeTestService] Error querying Supabase. Using local cache:", err);
    } finally {
      inFlightQuestionRequests.delete(cacheKey);
    }

    questionSessionCache.set(cacheKey, cachedTest);
    return cachedTest;
  })();

  inFlightQuestionRequests.set(cacheKey, fetchPromise);
  return fetchPromise;
}

/**
 * Optimized question fetcher that loads the complete practice test for a topic in a single request.
 * Supports session caching, promise deduplication, and overload parameters.
 */
export async function fetchQuestions(
  classGradeOrTopicId: string,
  subject?: string,
  chapterNo?: number,
  topicName?: string,
  testType: "topic" | "full_chapter" = "topic",
  options?: { publishedOnly?: boolean }
): Promise<ParsedAssessmentQuestion[]> {
  let classGrade = classGradeOrTopicId;
  if (classGradeOrTopicId && classGradeOrTopicId.includes("__") && !subject) {
    const parts = classGradeOrTopicId.split("__");
    classGrade = parts[0] || "";
    subject = parts[1] || "";
    chapterNo = parseInt((parts[2] || "").replace("ch", ""), 10) || 1;
    topicName = parts.slice(3).join("__");
  }

  if (testType === "full_chapter") {
    return getFullChapterQuestions(classGrade, subject || "", chapterNo || 1, options);
  }

  const topicTest = await getTopicPracticeTest(
    classGrade,
    subject || "",
    chapterNo || 1,
    topicName || "",
    options
  );

  return topicTest?.questions || [];
}

function isTestBankEqual(
  bankA: Record<string, TopicPracticeTest>,
  bankB: Record<string, TopicPracticeTest>
): boolean {
  if (!bankA || !bankB) return false;
  const keysA = Object.keys(bankA);
  const keysB = Object.keys(bankB);
  if (keysA.length !== keysB.length) return false;

  for (const key of keysA) {
    const testA = bankA[key];
    const testB = bankB[key];
    if (!testB) return false;

    const qA = testA?.questions || [];
    const qB = testB?.questions || [];
    if (qA.length !== qB.length) return false;

    for (let i = 0; i < qA.length; i++) {
      if (qA[i]?.id !== qB[i]?.id || qA[i]?.published !== qB[i]?.published) {
        return false;
      }
    }

    if (testA.updatedAt !== testB.updatedAt) return false;
  }

  return true;
}

let activeFetchPromise: Promise<Record<string, TopicPracticeTest>> | null = null;

/**
 * Fetches all topic assessment questions from Supabase DB (Single Source of Truth) and populates the local test bank cache.
 */
export async function fetchAllPracticeTestsFromSupabase(): Promise<Record<string, TopicPracticeTest>> {
  if (activeFetchPromise) {
    return activeFetchPromise;
  }

  activeFetchPromise = (async () => {
    try {
      const { data, error } = await supabase
        .from("topic_assessment_questions")
        .select("*")
        .order("order_index", { ascending: true })
        .range(0, 9999);

      if (!error && Array.isArray(data)) {
        const dbBank: Record<string, TopicPracticeTest> = {};

        if (data.length > 0) {
          const testMap: Record<string, { rows: any[]; questions: ParsedAssessmentQuestion[] }> = {};

          data.forEach((row) => {
            const classGrade = String(row.class_id || "").trim();
            const subject = String(row.subject_id || "").trim();
            const chapterNo = Number(row.chapter_id) || 1;
            const topicName = String(row.topic_id || "").trim();
            const testId = buildTopicTestId(classGrade, subject, chapterNo, topicName);

            if (!testMap[testId]) {
              testMap[testId] = { rows: [], questions: [] };
            }
            testMap[testId].rows.push(row);
            testMap[testId].questions.push(fromSupabaseRow(row, `Chapter ${chapterNo}`));
          });

          for (const testId of Object.keys(testMap)) {
            const item = testMap[testId];
            const firstRow = item.rows[0];
            const classGrade = String(firstRow.class_id || "").trim();
            const subject = String(firstRow.subject_id || "").trim();
            const chapterNo = Number(firstRow.chapter_id) || 1;
            const topicName = String(firstRow.topic_id || "").trim();

            const resolvedQuestions = await resolveQuestionImageUrls(item.questions);

            dbBank[testId] = {
              id: testId,
              classGrade,
              subject,
              chapterNo,
              chapterName: `Chapter ${chapterNo}`,
              topicName,
              rawText: firstRow.raw_text || "",
              questions: resolvedQuestions,
              createdAt: firstRow.created_at || new Date().toISOString(),
              updatedAt: firstRow.updated_at || new Date().toISOString(),
              uploadedBy: "Admin",
            };
          }

          // Merge DB bank with existing local cache to prevent losing locally stored tests
          const currentLocal = getLocalTestBank();
          const mergedBank = { ...currentLocal, ...dbBank };
          const hasChanged = !isTestBankEqual(currentLocal, mergedBank);

          if (hasChanged) {
            saveLocalTestBank(mergedBank, { silent: false });
            syncTestBankToSupabaseStorage(mergedBank).catch(() => {});
          } else {
            memoryTestBank = mergedBank;
          }

          return mergedBank;
        } else {
          // DB returned 0 rows, check local bank
          const localBank = getLocalTestBank();
          if (Object.keys(localBank).length > 0) {
            console.warn("[PracticeTestService] Supabase DB returned 0 rows. Retaining local test bank with", Object.keys(localBank).length, "topics.");
            return localBank;
          }
        }
      }
    } catch (err) {
      console.warn("[PracticeTestService] Error fetching practice tests from DB:", err);
    } finally {
      activeFetchPromise = null;
    }

    // Fallback to Storage or local storage if DB query failed
    const bank = getLocalTestBank();
    try {
      const storageBank = await fetchTestBankFromSupabaseStorage();
      if (storageBank && typeof storageBank === "object" && Object.keys(storageBank).length > 0) {
        const hasChanged = !isTestBankEqual(bank, storageBank);
        if (hasChanged) {
          saveLocalTestBank(storageBank, { silent: false });
        } else {
          memoryTestBank = storageBank;
        }
        return storageBank;
      }
    } catch (err) {
      console.warn("[PracticeTestService] Storage fetch warning:", err);
    }

    return bank;
  })();

  return activeFetchPromise;
}

/**
 * Synchronously reads topic practice test from local cache (for instant rendering)
 */
export function getTopicPracticeTestSync(
  classGrade: string,
  subject: string,
  chapterNo: number,
  topicName: string,
  options?: { publishedOnly?: boolean }
): TopicPracticeTest | null {
  const testId = buildTopicTestId(classGrade, subject, chapterNo, topicName);
  const bank = getLocalTestBank();
  let test = bank[testId] || null;

  if (!test) {
    const allBankTests = Object.values(bank);
    test =
      allBankTests.find((t) =>
        isExactTopicMatch(
          classGrade,
          subject,
          chapterNo,
          topicName,
          t.classGrade,
          t.subject,
          t.chapterNo,
          t.topicName
        )
      ) || null;
  }

  if (!test) return null;
  if (options?.publishedOnly) {
    return {
      ...test,
      questions: (test.questions || []).filter((q) => q.published !== false),
    };
  }
  return test;
}

/**
 * Dynamically aggregates ALL published questions across all topics of a given Chapter directly from Supabase (or cached local bank).
 */
export async function getFullChapterQuestions(
  classGrade: string,
  subject: string,
  chapterNo: number,
  options: { publishedOnly?: boolean } = { publishedOnly: true }
): Promise<ParsedAssessmentQuestion[]> {
  try {
    const { data, error } = await supabase
      .from("topic_assessment_questions")
      .select("*")
      .eq("chapter_id", String(chapterNo))
      .order("order_index", { ascending: true })
      .range(0, 9999);

    if (!error && Array.isArray(data)) {
      const normClass = (classGrade || "").toLowerCase().replace(/[^a-z0-9]/g, "");
      const cleanNormClass = normClass.replace(/class/g, "");
      const normSubj = (subject || "").toLowerCase().replace(/[^a-z0-9]/g, "");

      const matchingRows = data.filter((row) => {
        if (Number(row.chapter_id) !== Number(chapterNo)) return false;

        if (options.publishedOnly !== false && row.published === false) {
          return false;
        }

        const rSubj = String(row.subject_id || "").toLowerCase().replace(/[^a-z0-9]/g, "");
        const subjMatch =
          !normSubj ||
          !rSubj ||
          normSubj === rSubj ||
          normSubj.includes(rSubj) ||
          rSubj.includes(normSubj);
        if (!subjMatch) return false;

        const rClass = String(row.class_id || "").toLowerCase().replace(/[^a-z0-9]/g, "");
        const cleanRClass = rClass.replace(/class/g, "");
        const classMatch =
          !normClass ||
          !rClass ||
          normClass === rClass ||
          cleanNormClass === cleanRClass ||
          normClass.includes(rClass) ||
          rClass.includes(normClass);
        if (!classMatch) return false;

        return true;
      });

      const parsedQuestions = matchingRows.map((row) => fromSupabaseRow(row, `Chapter ${chapterNo}`));
      return resolveQuestionImageUrls(parsedQuestions);
    }
  } catch (err) {
    console.warn("[PracticeTestService] Error fetching full chapter questions from Supabase:", err);
  }

  // Fallback: Aggregate from local cache
  return getFullChapterQuestionsSync(classGrade, subject, chapterNo, options);
}

/**
 * Synchronous version of getFullChapterQuestions for instant UI rendering
 */
export function getFullChapterQuestionsSync(
  classGrade: string,
  subject: string,
  chapterNo: number,
  options: { publishedOnly?: boolean } = { publishedOnly: true }
): ParsedAssessmentQuestion[] {
  const bank = getLocalTestBank();
  const aggregated: ParsedAssessmentQuestion[] = [];
  const normClass = (classGrade || "").toLowerCase().replace(/[^a-z0-9]/g, "");
  const cleanNormClass = normClass.replace(/class/g, "");
  const normSubj = (subject || "").toLowerCase().replace(/[^a-z0-9]/g, "");

  Object.values(bank).forEach((test) => {
    if (Number(test.chapterNo) !== Number(chapterNo)) return;

    const tSubj = (test.subject || "").toLowerCase().replace(/[^a-z0-9]/g, "");
    const subjMatch =
      !normSubj ||
      !tSubj ||
      normSubj === tSubj ||
      normSubj.includes(tSubj) ||
      tSubj.includes(normSubj);
    if (!subjMatch) return;

    const tClass = (test.classGrade || "").toLowerCase().replace(/[^a-z0-9]/g, "");
    const cleanTClass = tClass.replace(/class/g, "");
    const classMatch =
      !normClass ||
      !tClass ||
      normClass === tClass ||
      cleanNormClass === cleanTClass ||
      normClass.includes(tClass) ||
      tClass.includes(normClass);
    if (!classMatch) return;

    if (Array.isArray(test.questions)) {
      test.questions.forEach((q) => {
        if (options.publishedOnly === false || q.published !== false) {
          aggregated.push(q);
        }
      });
    }
  });

  return aggregated;
}

/**
 * Deletes a topic practice test completely from Supabase and local cache.
 */
export async function deleteTopicPracticeTest(
  classGrade: string,
  subject: string,
  chapterNo: number,
  topicName: string
): Promise<{ success: boolean; message: string }> {
  const testId = buildTopicTestId(classGrade, subject, chapterNo, topicName);

  // 1. Delete from Supabase DB FIRST
  const delRes = await deleteTopicFromSupabase(classGrade, subject, chapterNo, topicName);

  if (!delRes.success) {
    const isOffline = typeof navigator !== "undefined" && !navigator.onLine;
    if (isOffline) {
      const bank = getLocalTestBank();
      const existingTopic = bank[testId];
      const chapterName = existingTopic?.chapterName || `Chapter ${chapterNo}`;

      await queueOfflineDeleteTopic({ classGrade, subject, chapterNo, chapterName, topicName });

      removeLocalTopicCache(testId);
      delete bank[testId];

      Object.keys(bank).forEach((k) => {
        const t = bank[k];
        if (
          t &&
          isExactTopicMatch(
            classGrade,
            subject,
            chapterNo,
            topicName,
            t.classGrade,
            t.subject,
            t.chapterNo,
            t.topicName
          )
        ) {
          delete bank[k];
        }
      });

      saveLocalTestBank(bank);
      await syncTestBankToSupabaseStorage(bank).catch(() => {});

      if (typeof window !== "undefined") {
        window.dispatchEvent(new CustomEvent("practice-tests-updated"));
      }

      return {
        success: true,
        message: "Practice Test deletion queued for sync and local cache cleared while offline.",
      };
    }

    return { success: false, message: delRes.message || delRes.error || "Failed to delete topic practice test from Supabase." };
  }

  // 2. Remove queued offline sync items for this topic
  await removeSyncQueueItemsForTopic(classGrade, subject, chapterNo, topicName);

  // 3. Remove from local test bank (both exact testId and matching topic/chapter/subject/class entries)
  removeLocalTopicCache(testId);

  const bank = getLocalTestBank();
  delete bank[testId];

  Object.keys(bank).forEach((k) => {
    const t = bank[k];
    if (
      t &&
      isExactTopicMatch(
        classGrade,
        subject,
        chapterNo,
        topicName,
        t.classGrade,
        t.subject,
        t.chapterNo,
        t.topicName
      )
    ) {
      delete bank[k];
    }
  });

  saveLocalTestBank(bank);

  // Clear in-memory question session caches
  questionSessionCache.clear();
  inFlightQuestionRequests.clear();

  // Sync updated bank (with deleted test removed) to Supabase Storage
  await syncTestBankToSupabaseStorage(bank).catch((err) => {
    console.warn("[PracticeTestService] Error syncing bank to Supabase Storage during delete:", err);
  });

  // 4. Re-fetch fresh bank from Supabase DB to ensure complete cache consistency
  await fetchAllPracticeTestsFromSupabase().catch(() => {});

  // 5. Dispatch update event for UI listeners
  await notifyPracticeTestRealtimeSync({ testId, action: "delete_topic" });

  return { success: true, message: "Practice Test deleted successfully." };
}

export async function deleteTopicPracticeTestDirect(
  classGrade: string,
  subject: string,
  chapterNo: number,
  topicName: string
): Promise<{ success: boolean; message?: string; error?: string }> {
  return deleteTopicFromSupabase(classGrade, subject, chapterNo, topicName);
}

/**
 * Updates a single question record in Supabase and local cache.
 */
export async function updateAssessmentQuestion(
  questionId: string,
  updates: Partial<ParsedAssessmentQuestion>
): Promise<{ success: boolean; message: string }> {
  // 1. Update in local cache first
  const bank = getLocalTestBank();
  let foundTest: TopicPracticeTest | null = null;
  let questionIndex = -1;

  for (const t of Object.values(bank)) {
    const idx = (t.questions || []).findIndex((q) => q.id === questionId);
    if (idx !== -1) {
      foundTest = t;
      questionIndex = idx;
      break;
    }
  }

  if (foundTest && questionIndex !== -1) {
    foundTest.questions[questionIndex] = {
      ...foundTest.questions[questionIndex],
      ...updates,
      updatedAt: new Date().toISOString(),
    };
    updateLocalTopicCache(foundTest);

    // Re-push entire topic to Supabase DB so that image_url and all fields are saved correctly
    await pushTopicToSupabase(
      {
        classGrade: foundTest.classGrade,
        subject: foundTest.subject,
        chapterNo: foundTest.chapterNo,
        chapterName: foundTest.chapterName,
        topicName: foundTest.topicName,
      },
      foundTest.questions,
      foundTest.rawText || ""
    ).catch((err) => console.warn("[PracticeTestService] Error pushing updated topic to Supabase DB:", err));
  } else {
    // Attempt single-row update in Supabase DB by id
    try {
      const dbUpdates: any = { updated_at: new Date().toISOString() };
      if (updates.question !== undefined) dbUpdates.question = updates.question;
      if (updates.options !== undefined) dbUpdates.options = updates.options;
      if (updates.correctAnswer !== undefined) dbUpdates.correct_answer = updates.correctAnswer;
      if (updates.published !== undefined) dbUpdates.published = updates.published;
      if (updates.type !== undefined) dbUpdates.question_type = updates.type === "mcq" ? "MCQ" : "TRUE_FALSE";
      if (updates.imageUrl !== undefined) dbUpdates.image_url = updates.imageUrl;
      if (updates.imageLabel !== undefined) dbUpdates.image_label = updates.imageLabel;
      if (updates.imagePosition !== undefined) dbUpdates.image_position = updates.imagePosition;

      await supabase.from("topic_assessment_questions").update(dbUpdates).eq("id", questionId);
    } catch (err) {
      console.warn("[PracticeTestService] Error updating question row in Supabase:", err);
    }
  }

  // 2. Sync updated bank to Supabase Storage backup
  await syncTestBankToSupabaseStorage(getLocalTestBank()).catch(() => {});

  await notifyPracticeTestRealtimeSync({ questionId, action: "update_question" });

  return { success: true, message: "Question updated successfully." };
}

/**
 * Deletes a single question from Supabase and local cache.
 */
export async function deleteAssessmentQuestion(
  questionId: string
): Promise<{ success: boolean; message: string }> {
  console.log("[PracticeTestService] Deleting question from Supabase with id:", questionId);

  function removeQuestionFromLocalCache(id: string): void {
    const bank = getLocalTestBank();
    let modified = false;

    for (const k of Object.keys(bank)) {
      const t = bank[k];
      if (t && Array.isArray(t.questions)) {
        const filtered = t.questions.filter((q) => q.id !== id);
        if (filtered.length !== t.questions.length) {
          modified = true;
          if (filtered.length === 0) {
            delete bank[k];
            removeLocalTopicCache(k);
          } else {
            t.questions = filtered;
            updateLocalTopicCache(t);
          }
        }
      }
    }

    if (modified) {
      saveLocalTestBank(bank);
    }
  }

  // 1. Delete from Supabase DB
  const isOffline = typeof navigator !== "undefined" && !navigator.onLine;

  try {
    const { error } = await supabase.from("topic_assessment_questions").delete().eq("id", questionId);
    if (error) {
      console.error("[PracticeTestService] Delete question SQL error:", error, "Filters:", { id: questionId });
      if (isOffline) {
        await queueOfflineDeleteQuestion(questionId);
        await removeSyncQueueItemsForQuestion(questionId);
        removeQuestionFromLocalCache(questionId);
        await syncTestBankToSupabaseStorage(getLocalTestBank()).catch(() => {});
        if (typeof window !== "undefined") {
          window.dispatchEvent(new CustomEvent("practice-tests-updated"));
        }
        return {
          success: true,
          message: "Question deletion queued for sync and local cache cleared while offline.",
        };
      }
      return { success: false, message: error.message || "Failed to delete question from Supabase." };
    }

    await removeSyncQueueItemsForQuestion(questionId);

    // Post-delete verification check
    const { data: checkData, error: checkErr } = await supabase
      .from("topic_assessment_questions")
      .select("id")
      .eq("id", questionId);

    if (!checkErr && checkData && checkData.length > 0) {
      console.error("[PracticeTestService] Verification failed: Question row still exists in Supabase!", { questionId });
      if (isOffline) {
        await queueOfflineDeleteQuestion(questionId);
        await removeSyncQueueItemsForQuestion(questionId);
        removeQuestionFromLocalCache(questionId);
        await syncTestBankToSupabaseStorage(getLocalTestBank()).catch(() => {});
        if (typeof window !== "undefined") {
          window.dispatchEvent(new CustomEvent("practice-tests-updated"));
        }
        return {
          success: true,
          message: "Question deletion queued for sync and local cache cleared while offline.",
        };
      }
      return { success: false, message: "Deletion failed: Question still exists in Supabase table public.topic_assessment_questions." };
    }
  } catch (err: any) {
    console.error("[PracticeTestService] Exception deleting question from Supabase:", err);
    if (isOffline) {
      await queueOfflineDeleteQuestion(questionId);
      await removeSyncQueueItemsForQuestion(questionId);
      removeQuestionFromLocalCache(questionId);
      await syncTestBankToSupabaseStorage(getLocalTestBank()).catch(() => {});
      if (typeof window !== "undefined") {
        window.dispatchEvent(new CustomEvent("practice-tests-updated"));
      }
      return {
        success: true,
        message: "Question deletion queued for sync and local cache cleared while offline.",
      };
    }
    return { success: false, message: err.message || "Failed to delete question from Supabase." };
  }

  // 2. Remove question from local memory cache
  removeQuestionFromLocalCache(questionId);
  saveLocalTestBank(getLocalTestBank());

  // 3. Refresh local cache from Supabase DB
  await fetchAllPracticeTestsFromSupabase().catch(() => {});

  // 4. Dispatch update event
  await notifyPracticeTestRealtimeSync({ questionId, action: "delete_question" });

  return { success: true, message: "Question deleted successfully." };
}

/**
 * Reorders questions inside a topic test in Supabase and local cache.
 */
export async function reorderAssessmentQuestions(
  classGrade: string,
  subject: string,
  chapterNo: number,
  topicName: string,
  reorderedQuestions: ParsedAssessmentQuestion[]
): Promise<{ success: boolean }> {
  const testId = buildTopicTestId(classGrade, subject, chapterNo, topicName);
  const bank = getLocalTestBank();
  const test = bank[testId];

  if (test) {
    test.questions = reorderedQuestions.map((q, idx) => ({
      ...q,
      orderIndex: idx + 1,
    }));
    updateLocalTopicCache(test);
  }

  try {
    for (let i = 0; i < reorderedQuestions.length; i++) {
      const q = reorderedQuestions[i];
      await supabase
        .from("topic_assessment_questions")
        .update({ order_index: i + 1, updated_at: new Date().toISOString() })
        .eq("id", q.id);
    }
  } catch (err) {
    console.warn("[PracticeTestService] Error reordering questions in Supabase:", err);
  }

  await syncTestBankToSupabaseStorage(bank).catch(() => {});

  await notifyPracticeTestRealtimeSync({ testId, action: "reorder_questions" });

  return { success: true };
}
