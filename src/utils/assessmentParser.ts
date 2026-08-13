import { ParsedAssessmentQuestion, TopicPracticeTest, TestAttemptRecord } from "../types";

export interface ParseResult {
  success: boolean;
  questions: ParsedAssessmentQuestion[];
  errors: string[];
}

const TESTS_STORAGE_KEY = "tuition_topic_practice_tests_bank";
const ATTEMPTS_STORAGE_KEY = "tuition_student_test_attempts";

/**
 * Normalizes test ID for topic practice tests
 */
export function buildTopicTestId(
  classGrade: string = "",
  subject: string = "",
  chapterNo: number = 0,
  topicName: string = ""
): string {
  const normClass = (classGrade || "").toLowerCase().replace(/\s+/g, "_");
  const normSubj = (subject || "").toLowerCase().replace(/\s+/g, "_");
  const normTopic = (topicName || "").toLowerCase().replace(/[^a-z0-9]/g, "_");
  return `${normClass}__${normSubj}__ch${chapterNo}__${normTopic}`;
}

/**
 * Parses raw pasted text into structured MCQ and True/False questions.
 */
export function parseAssessmentText(
  rawText: string,
  context: {
    classGrade: string;
    subject: string;
    chapterNo: number;
    chapterName: string;
    topicName: string;
  }
): ParseResult {
  const errors: string[] = [];
  const questions: ParsedAssessmentQuestion[] = [];

  const text = rawText.trim();
  if (!text) {
    return {
      success: false,
      questions: [],
      errors: ["Please enter or paste questions text into the editor."]
    };
  }

  // Pre-process inline options (e.g., "A) Opt1  B) Opt2  C) Opt3  D) Opt4")
  const processedText = text.replace(
    /\s+([A-Ea-e1-5][\.\)\:\-])(?=\s+[^\n]+)/g,
    (match) => {
      return "\n" + match.trim();
    }
  );

  // Split into raw lines
  const lines = processedText.split("\n").map((l) => l.trim()).filter(Boolean);
  if (lines.length === 0) {
    return {
      success: false,
      questions: [],
      errors: ["No valid text lines found in input."]
    };
  }

  console.log(`[AssessmentParser] Beginning parse for ${lines.length} lines of text...`);

  let currentSection: "mcq" | "true_false" | "unknown" = "unknown";
  const blocks: { section: "mcq" | "true_false" | "unknown"; lines: string[] }[] = [];
  let currentBlock: string[] = [];
  let hasParsingStarted = false;

  // Question starting line regex (detects 1., 1), Q1., Q1), Q1:, Q.1, Question 1:, (1), [1], etc.)
  const isQuestionHeader = (line: string): boolean => {
    const trimmed = line.trim();
    return (
      /^(?:\*|\_)*\s*(?:[Qq]\.?\s*\d+|\d+|[Qq]uestion\s*\d+|\(?[Qq]?\d+\)?)\s*[\.\)\:\-]\s+/i.test(trimmed) ||
      /^(?:\*|\_)*\s*(?:[Qq]\.?\s*\d+|\d+|[Qq]uestion\s*\d+|\(?[Qq]?\d+\)?)\s*[\.\)\:\-]$/i.test(trimmed) ||
      /^\s*\(\d+\)\s+/i.test(trimmed) ||
      /^\s*\[\d+\]\s+/i.test(trimmed) ||
      /^\s*\[Image(?:\s+Upload)?:\s*[^\]]+\]/i.test(trimmed) ||
      /^\s*Diagram:\s*/i.test(trimmed)
    );
  };

  const isHeadingOrDivider = (line: string): boolean => {
    const lower = line.trim().toLowerCase();
    if (!lower) return true;
    if (/^[⸻\-\=\_\*]{2,}$/.test(lower)) return true;
    if (
      lower.startsWith("chapter") ||
      lower.startsWith("topic") ||
      lower.startsWith("class:") ||
      lower.startsWith("subject:") ||
      lower.startsWith("sample test") ||
      lower.startsWith("general instruction") ||
      lower.startsWith("time:") ||
      lower.startsWith("max marks") ||
      lower.startsWith("total marks") ||
      lower.startsWith("marks:")
    ) {
      return true;
    }
    if (
      lower === "mcqs" ||
      lower === "mcq" ||
      lower === "multiple choice questions" ||
      lower === "multiple choice" ||
      lower === "assertion & reasoning" ||
      lower === "assertion and reasoning" ||
      lower === "true / false" ||
      lower === "true/false" ||
      lower === "true or false" ||
      lower === "t/f" ||
      lower.startsWith("section ") ||
      lower.startsWith("part ")
    ) {
      return true;
    }
    return false;
  };

  const isOptionLine = (line: string): boolean => {
    return (
      /^(?:Option\s+[A-Ea-e1-5]|[A-Ea-e1-5][\.\)\:\-]|[\(\[]?[A-Ea-e1-5][\)\Rot]?)\s+/i.test(line) ||
      /^(?:True|False)\s*[✅❌]?$/i.test(line) ||
      /^(?:[Aa]nswer|[Aa]ns)\s*[:\-]\s*/i.test(line)
    );
  };

  const isAnswerLine = (line: string): boolean => {
    return /^(?:Correct\s*)?Ans(?:wer)?\s*[:\-]/i.test(line);
  };

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];

    // If parsing has NOT started yet, ignore all lines until the first numbered question
    if (!hasParsingStarted) {
      if (isQuestionHeader(line) && !isHeadingOrDivider(line)) {
        hasParsingStarted = true;
        currentBlock = [line];
      }
      continue;
    }

    // Parsing HAS started:
    // If this line is a new numbered question header:
    if (isQuestionHeader(line) && !isHeadingOrDivider(line)) {
      if (currentBlock.length > 0) {
        blocks.push({ section: currentSection, lines: currentBlock });
      }
      currentBlock = [line];
      continue;
    }

    // If it's a heading or divider line inside or between questions:
    if (isHeadingOrDivider(line)) {
      const lower = line.trim().toLowerCase();
      if (lower.includes("true") && (lower.includes("false") || lower.includes("t/f"))) {
        currentSection = "true_false";
      } else if (lower.includes("mcq") || lower.includes("multiple choice")) {
        currentSection = "mcq";
      }
      continue;
    }

    // Regular line inside current question (options, answers, images, text)
    currentBlock.push(line);
  }

  if (currentBlock.length > 0) {
    blocks.push({ section: currentSection, lines: currentBlock });
  }

  console.log(`[AssessmentParser] Formed ${blocks.length} raw question blocks.`);

  // Process each block
  blocks.forEach((blockObj, index) => {
    let blockLines = [...blockObj.lines];
    const blockSection = blockObj.section;
    const fullBlockText = blockLines.join("\n");
    const firstLine = blockLines[0] || "";

    // Check if firstLine is just a leftover metadata line
    const firstLower = firstLine.toLowerCase();
    if (
      firstLower.startsWith("chapter:") ||
      firstLower.startsWith("topic:") ||
      firstLower.startsWith("class:") ||
      firstLower.startsWith("subject:") ||
      firstLower === "sample test"
    ) {
      return;
    }

    // 1. Extract explicit "Correct Answer: X" line if present
    let explicitCorrectAnswer = "";
    const remainingLines: string[] = [];
    blockLines.forEach((l) => {
      const caMatch = l.match(/^(?:Correct\s*)?Ans(?:wer)?\s*[:\-]\s*(?:Option\s*)?([A-Ea-e1-5]|True|False|[Aa]|[Bb]|[Cc]|[Dd])/i);
      if (caMatch) {
        explicitCorrectAnswer = caMatch[1].trim();
      } else {
        remainingLines.push(l);
      }
    });
    blockLines = remainingLines;

    // 2. Extract Image marker [Image Upload: ...] or [Image: ...] if present
    let extractedImageLabel = "";
    const cleanLinesAfterImage: string[] = [];
    blockLines.forEach((l) => {
      const imgMatch = l.match(/\[Image(?:\s+Upload)?:\s*([^\]]+)\]/i);
      if (imgMatch) {
        extractedImageLabel = imgMatch[1].trim();
        const lineWithoutTag = l.replace(/\[Image(?:\s+Upload)?:\s*([^\]]+)\]/gi, "").trim();
        if (lineWithoutTag) cleanLinesAfterImage.push(lineWithoutTag);
      } else {
        cleanLinesAfterImage.push(l);
      }
    });
    blockLines = cleanLinesAfterImage;

    if (blockLines.length === 0) return;

    // Detect option lines inside blockLines
    const optIndices: number[] = [];
    blockLines.forEach((l, idx) => {
      if (/^(?:Option\s+[A-Ea-e1-5]|[A-Ea-e1-5][\.\)\:\-]|[\(\[]?[A-Ea-e1-5][\)\Rot]?)\s+/i.test(l)) {
        optIndices.push(idx);
      }
    });

    const hasMCQOptions = optIndices.length > 0;
    const isTFPattern =
      !hasMCQOptions &&
      (fullBlockText.includes("True") ||
        fullBlockText.includes("False") ||
        fullBlockText.includes("— True") ||
        fullBlockText.includes("— False") ||
        fullBlockText.includes("✅") ||
        fullBlockText.includes("❌") ||
        explicitCorrectAnswer.toLowerCase() === "true" ||
        explicitCorrectAnswer.toLowerCase() === "false");

    let isMCQ = hasMCQOptions;
    let isTF = isTFPattern;

    if (!isMCQ && !isTF) {
      if (blockSection === "mcq") isMCQ = true;
      else if (blockSection === "true_false") isTF = true;
      else isMCQ = true; // Default fallback to MCQ/Question
    }

    if (isMCQ && optIndices.length > 0) {
      const firstOptIdx = optIndices[0];

      // Question lines before first option
      const rawQLines = blockLines.slice(0, firstOptIdx).filter((l) => l.toLowerCase() !== "question:");
      let qText = rawQLines
        .map((l, idx) =>
          idx === 0
            ? l.replace(/^(?:\*|\_)*\s*(?:[Qq]\.?\s*\d+|\d+|[Qq]uestion\s*\d+|\(?[Qq]?\d+\)?)\s*[\.\)\:\-]?\s*/i, "")
            : l
        )
        .join("\n")
        .trim();

      if (!qText) {
        qText = `Question ${index + 1}`;
      }

      const optionLines = blockLines.slice(firstOptIdx);
      const options: string[] = [];
      let correctAnswerLetter = "";

      optionLines.forEach((optLine) => {
        const match = optLine.match(/^(?:Option\s+([A-Ea-e1-5])|([A-Ea-e1-5])[\.\)\:\-]|[\(\[]?([A-Ea-e1-5])[\)\Rot]?)\s+(.*)$/i);
        if (match) {
          let rawLetter = (match[1] || match[2] || match[3] || "A").toUpperCase();
          // Convert numeric options 1->A, 2->B, 3->C, 4->D
          if (["1", "2", "3", "4", "5"].includes(rawLetter)) {
            const numMap: Record<string, string> = { "1": "A", "2": "B", "3": "C", "4": "D", "5": "E" };
            rawLetter = numMap[rawLetter] || "A";
          }

          let optVal = match[4].trim();
          const hasCheck = optVal.includes("✅") || /\(correct\)/i.test(optVal) || /\(answer\)/i.test(optVal);

          optVal = optVal
            .replace(/[✅❌]/g, "")
            .replace(/\s*\(trap\)/gi, "")
            .replace(/\s*\(correct\)/gi, "")
            .replace(/\s*\(answer\)/gi, "")
            .trim();

          if (hasCheck) {
            correctAnswerLetter = rawLetter;
          }

          options.push(`${rawLetter}. ${optVal}`);
        } else if (optLine.trim()) {
          options.push(optLine.trim());
        }
      });

      // Answer fallback matching
      if (!correctAnswerLetter && explicitCorrectAnswer) {
        let cleanExplicit = explicitCorrectAnswer.toUpperCase();
        if (["1", "2", "3", "4", "5"].includes(cleanExplicit)) {
          const numMap: Record<string, string> = { "1": "A", "2": "B", "3": "C", "4": "D", "5": "E" };
          cleanExplicit = numMap[cleanExplicit] || "A";
        }
        if (/^[A-E]$/.test(cleanExplicit)) {
          correctAnswerLetter = cleanExplicit;
        }
      }

      // Safeguard options: ensure at least 2 options
      if (options.length < 2) {
        if (options.length === 1) {
          options.push("B. None of the above");
        } else {
          options.push("A. Option A");
          options.push("B. Option B");
        }
        errors.push(`Question #${index + 1}: Incomplete options detected, auto-generated placeholder options.`);
      }

      // Safeguard correct answer: fallback to first option letter if missing
      if (!correctAnswerLetter) {
        const firstLetter = options[0].charAt(0);
        correctAnswerLetter = /^[A-E]$/i.test(firstLetter) ? firstLetter.toUpperCase() : "A";
        errors.push(`Question #${index + 1}: Missing correct answer tag, default set to Option ${correctAnswerLetter}.`);
      }

      questions.push({
        id: `q_mcq_${index + 1}_${Math.random().toString(36).substring(2, 7)}`,
        classGrade: context.classGrade,
        subject: context.subject,
        chapterNo: context.chapterNo,
        chapterName: context.chapterName,
        topicName: context.topicName,
        type: "mcq",
        question: qText,
        options,
        correctAnswer: correctAnswerLetter,
        imageLabel: extractedImageLabel || undefined,
        rawText: fullBlockText
      });
    } else if (isTF) {
      let rawStatement = blockLines
        .filter((l) => l.toLowerCase() !== "question:")
        .join(" ")
        .replace(/^(?:\*|\_)*\s*(?:[Qq]\.?\s*\d+|\d+|[Qq]uestion\s*\d+|\(?[Qq]?\d+\)?)\s*[\.\)\:\-]?\s*/i, "")
        .trim();

      let correctAnswer = "";
      const lowerStmt = rawStatement.toLowerCase();

      if (
        rawStatement.includes("True ✅") ||
        rawStatement.includes("— True") ||
        rawStatement.includes("- True") ||
        (rawStatement.includes("True") && rawStatement.includes("✅"))
      ) {
        correctAnswer = "True";
      } else if (
        rawStatement.includes("False ❌") ||
        rawStatement.includes("— False") ||
        rawStatement.includes("- False") ||
        (rawStatement.includes("False") && (rawStatement.includes("❌") || rawStatement.includes("✅")))
      ) {
        correctAnswer = "False";
      } else if (lowerStmt.endsWith("true")) {
        correctAnswer = "True";
      } else if (lowerStmt.endsWith("false")) {
        correctAnswer = "False";
      } else if (
        explicitCorrectAnswer &&
        (explicitCorrectAnswer.toLowerCase() === "true" ||
          explicitCorrectAnswer.toLowerCase() === "false" ||
          explicitCorrectAnswer.toLowerCase() === "t" ||
          explicitCorrectAnswer.toLowerCase() === "f")
      ) {
        const ca = explicitCorrectAnswer.toLowerCase();
        correctAnswer = ca === "true" || ca === "t" ? "True" : "False";
      }

      let cleanQuestion = rawStatement
        .replace(/—\s*(True|False)\s*[✅❌]?/gi, "")
        .replace(/-\s*(True|False)\s*[✅❌]?/gi, "")
        .replace(/\b(True|False)\s*[✅❌]?/gi, "")
        .replace(/[✅❌]/g, "")
        .replace(/\s*\(trap\)/gi, "")
        .replace(/\s*\(correct\)/gi, "")
        .replace(/\s*\(answer\)/gi, "")
        .trim();

      if (!cleanQuestion) {
        cleanQuestion = rawStatement.replace(/[✅❌]/g, "").trim();
      }

      if (!correctAnswer) {
        correctAnswer = "True";
        errors.push(`Question #${index + 1}: True/False answer tag missing, default set to True.`);
      }

      questions.push({
        id: `q_tf_${index + 1}_${Math.random().toString(36).substring(2, 7)}`,
        classGrade: context.classGrade,
        subject: context.subject,
        chapterNo: context.chapterNo,
        chapterName: context.chapterName,
        topicName: context.topicName,
        type: "true_false",
        question: cleanQuestion || `Question ${index + 1}`,
        options: ["True", "False"],
        correctAnswer,
        imageLabel: extractedImageLabel || undefined,
        rawText: fullBlockText
      });
    } else {
      // General Fallback for any unformatted text block: create an MCQ or True/False question
      const qText = blockLines
        .join("\n")
        .replace(/^(?:\*|\_)*\s*(?:[Qq]\.?\s*\d+|\d+|[Qq]uestion\s*\d+|\(?[Qq]?\d+\)?)\s*[\.\)\:\-]?\s*/i, "")
        .trim();

      questions.push({
        id: `q_mcq_${index + 1}_${Math.random().toString(36).substring(2, 7)}`,
        classGrade: context.classGrade,
        subject: context.subject,
        chapterNo: context.chapterNo,
        chapterName: context.chapterName,
        topicName: context.topicName,
        type: "mcq",
        question: qText || `Question ${index + 1}`,
        options: ["A. True", "B. False"],
        correctAnswer: "A",
        imageLabel: extractedImageLabel || undefined,
        rawText: fullBlockText
      });
      errors.push(`Question #${index + 1}: Unformatted question parsed using general fallback.`);
    }
  });

  console.log(`[AssessmentParser] Successfully extracted ${questions.length} questions from ${blocks.length} blocks.`);

  return {
    success: questions.length > 0,
    questions,
    errors
  };
}

// ----------------------------------------------------
// LOCAL STORAGE & PERSISTENCE HELPERS
// ----------------------------------------------------

import { 
  getLocalTestBank as getAllPracticeTests,
  getTopicPracticeTestSync as getTopicPracticeTest,
  getTopicPracticeTestSync,
  getTopicPracticeTest as getTopicPracticeTestAsync,
  saveTopicPracticeTest as saveServiceTopicTest,
  deleteTopicPracticeTest as deleteServiceTopicTest,
  getFullChapterQuestionsSync as getFullChapterQuestions,
  fetchAllPracticeTestsFromSupabase
} from "../lib/practiceTestService";

export { 
  getAllPracticeTests, 
  getTopicPracticeTest, 
  getTopicPracticeTestSync,
  getTopicPracticeTestAsync,
  getFullChapterQuestions, 
  fetchAllPracticeTestsFromSupabase 
};

export function saveTopicPracticeTest(test: TopicPracticeTest): void {
  saveServiceTopicTest(
    {
      classGrade: test.classGrade,
      subject: test.subject,
      chapterNo: test.chapterNo,
      chapterName: test.chapterName,
      topicName: test.topicName,
      rawText: test.rawText
    },
    test.questions
  );
}

export function deleteTopicPracticeTest(testIdOrTopic: string): void {
  // If testId is passed in format class__subj__ch1__topic
  const parts = testIdOrTopic.split("__");
  if (parts.length >= 4) {
    const classGrade = parts[0].replace(/_/g, " ");
    const subject = parts[1].replace(/_/g, " ");
    const chapterNo = parseInt(parts[2].replace("ch", ""), 10) || 1;
    const topicName = parts.slice(3).join("__");
    deleteServiceTopicTest(classGrade, subject, chapterNo, topicName);
  } else {
    // Fallback in-memory cache deletion
    const all = getAllPracticeTests();
    delete all[testIdOrTopic];
    if (typeof window !== "undefined") {
      window.dispatchEvent(new CustomEvent("practice-tests-updated"));
    }
  }
}

// ----------------------------------------------------
// TEST ATTEMPTS HELPERS
// ----------------------------------------------------

import { 
  getLocalTestAttempts, 
  saveTestAttemptDoc, 
  subscribeToTestAttempts,
  saveLocalTestAttemptsCache
} from "../lib/firestoreService";
import { 
  syncTestAttemptsToSupabaseStorage, 
  fetchTestAttemptsFromSupabaseStorage 
} from "../lib/practiceTestService";
import {
  savePracticeTestAttemptToSupabase,
  fetchStudentTestAttemptsFromSupabase,
  getCachedAttemptsFromMemory
} from "../lib/testScorePersistence";

export { subscribeToTestAttempts, fetchStudentTestAttemptsFromSupabase };

if (typeof window !== "undefined") {
  (async () => {
    try {
      const remote = await fetchTestAttemptsFromSupabaseStorage();
      if (remote && remote.length > 0) {
        const local = getLocalTestAttempts();
        const mergedMap = new Map<string, TestAttemptRecord>();
        for (const item of remote) {
          if (item && item.id) mergedMap.set(item.id, item);
        }
        for (const item of local) {
          if (item && item.id) mergedMap.set(item.id, item);
        }
        const mergedList = Array.from(mergedMap.values());
        mergedList.sort((a, b) => (b.timestamp || 0) - (a.timestamp || 0));
        saveLocalTestAttemptsCache(mergedList);
      }
    } catch (e) {
      console.warn("[AssessmentParser] Bootstrapping attempts from Supabase storage warning:", e);
    }
  })();
}

export function getAllTestAttempts(): TestAttemptRecord[] {
  return getCachedAttemptsFromMemory();
}

export function saveTestAttempt(attempt: TestAttemptRecord): void {
  savePracticeTestAttemptToSupabase(attempt).catch((err) => {
    console.warn("[AssessmentParser] saveTestAttempt error:", err);
  });
}

export function getStudentTestAttempts(
  studentIdentifier: string = "",
  classGrade?: string,
  subject?: string,
  chapterNo?: number,
  topicName?: string,
  testType?: "topic" | "full_chapter"
): TestAttemptRecord[] {
  const all = getAllTestAttempts();
  const normIdent = (studentIdentifier || "").toLowerCase().trim();
  const normClass = (classGrade || "").toLowerCase().trim();
  const normSubj = (subject || "").toLowerCase().trim();
  const normTopic = (topicName || "").toLowerCase().trim().replace(/[^a-z0-9]/g, "");

  // If memory has no attempts for student, trigger background fetch from Supabase
  if (studentIdentifier && all.length === 0) {
    fetchStudentTestAttemptsFromSupabase(studentIdentifier).catch(() => {});
  }

  return all.filter((a) => {
    if (studentIdentifier) {
      const matchId = (a.studentId || "").toLowerCase().trim() === normIdent;
      const matchName = (a.studentName || "").toLowerCase().trim() === normIdent;
      if (!matchId && !matchName) return false;
    }
    if (testType && a.testType !== testType) return false;
    if (classGrade) {
      const aClass = (a.classGrade || "").toLowerCase().trim();
      if (aClass && normClass && aClass !== normClass && !aClass.includes(normClass) && !normClass.includes(aClass)) return false;
    }
    if (subject) {
      const aSubj = (a.subject || "").toLowerCase().trim();
      if (aSubj && normSubj && aSubj !== normSubj && !aSubj.includes(normSubj) && !normSubj.includes(aSubj)) return false;
    }
    if (chapterNo !== undefined && Number(a.chapterNo) !== Number(chapterNo)) return false;
    if (topicName && testType === "topic") {
      const aTopic = (a.topicName || "").toLowerCase().trim().replace(/[^a-z0-9]/g, "");
      return aTopic === normTopic || aTopic.includes(normTopic) || normTopic.includes(aTopic);
    }
    return true;
  });
}

export function getStudentNextAttemptNumber(
  studentId: string,
  classGrade: string,
  subject: string,
  chapterNo: number,
  topicName: string,
  testType: "topic" | "full_chapter"
): number {
  const existing = getStudentTestAttempts(
    studentId,
    classGrade,
    subject,
    chapterNo,
    topicName,
    testType
  );
  return existing.length + 1;
}
