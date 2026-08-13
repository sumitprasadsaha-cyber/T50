import { Filesystem, Directory } from "@capacitor/filesystem";
import { FileOpener } from "@capacitor-community/file-opener";
import { Capacitor } from "@capacitor/core";
import { getPdfDownloadUrl } from "./pdfService";
import { getBucketName, sanitizeStoragePath } from "./storageService";
import { supabase } from "./supabaseClient";
import { dataUrlToBlob } from "../utils/pdfUtils";

export interface OpenPdfOptions {
  url: string;
  title?: string;
  storagePath?: string;
  bucket?: string;
  noteId?: string;
  fileName?: string;
  mimeType?: string;
  fileType?: "pdf" | "image" | string;
  onProgress?: (percent: number, statusText: string) => void;
}

export interface OpenPdfResult {
  success: boolean;
  message?: string;
  cachedPath?: string;
  isNative?: boolean;
}

/**
 * Determines whether a given note or file is an image based on fileType, mimeType, or filename extension.
 */
export function isImageFile(fileName?: string, url?: string, mimeType?: string, fileType?: string): boolean {
  if (fileType === "image") return true;
  if (mimeType && mimeType.toLowerCase().startsWith("image/")) return true;
  const str = (fileName || url || "").toLowerCase();
  return /\.(png|jpg|jpeg|webp|gif|bmp|svg)(\?.*)?$/i.test(str);
}

function getFileExtension(rawPathOrUrl: string, isImg: boolean): string {
  const clean = rawPathOrUrl.split("?")[0].split("#")[0];
  const match = clean.match(/\.([a-zA-Z0-9]+)$/);
  if (match) {
    const ext = match[1].toLowerCase();
    if (["pdf", "png", "jpg", "jpeg", "webp", "gif", "bmp", "svg"].includes(ext)) {
      return ext;
    }
  }
  return isImg ? "jpg" : "pdf";
}

function getMimeType(fileNameOrUrl: string, mimeType?: string, isImg?: boolean): string {
  if (mimeType && mimeType.trim()) return mimeType;
  const ext = getFileExtension(fileNameOrUrl, !!isImg);
  if (ext === "pdf") return "application/pdf";
  if (ext === "png") return "image/png";
  if (ext === "webp") return "image/webp";
  if (ext === "gif") return "image/gif";
  if (ext === "svg") return "image/svg+xml";
  if (ext === "jpg" || ext === "jpeg") return "image/jpeg";
  return isImg ? "image/jpeg" : "application/pdf";
}

/**
 * Generates a deterministic, filesystem-safe filename for caching a PDF or Image in Directory.Cache.
 */
export function getPdfCacheFileName(rawPathOrUrl: string, noteId?: string, isImg?: boolean, ext?: string): string {
  const identifier = noteId || rawPathOrUrl || "document";
  const cleanSlug = identifier
    .replace(/[^a-zA-Z0-9_-]/g, "_")
    .replace(/_+/g, "_")
    .substring(0, 60);

  let hash = 0;
  for (let i = 0; i < identifier.length; i++) {
    hash = (hash << 5) - hash + identifier.charCodeAt(i);
    hash |= 0;
  }
  const safeHash = Math.abs(hash).toString(36);

  if (isImg) {
    const extension = ext ? ext.replace(/^\./, "") : "jpg";
    return `img_cache_${cleanSlug}_${safeHash}.${extension}`;
  }
  return `pdf_cache_${cleanSlug}_${safeHash}.pdf`;
}

/**
 * Converts a Blob into a Base64 string required by Filesystem.writeFile.
 */
function blobToBase64(blob: Blob): Promise<string> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onerror = () => reject(new Error("Failed to read downloaded file bytes."));
    reader.onload = () => {
      const dataUrl = reader.result as string;
      const base64 = dataUrl.substring(dataUrl.indexOf(",") + 1);
      resolve(base64);
    };
    reader.readAsDataURL(blob);
  });
}

/**
 * Validates PDF header magic bytes (%PDF or Base64 equivalent JVBERi)
 */
async function validatePdfHeader(blob: Blob): Promise<boolean> {
  if (!blob || blob.size <= 0) return false;
  try {
    const headerSlice = blob.slice(0, 5);
    const headerText = await headerSlice.text();
    return headerText.startsWith("%PDF") || headerText.startsWith("JVBER");
  } catch {
    return false;
  }
}

/**
 * Downloads a PDF or Image from Supabase storage, caches it in the app's native Cache Directory,
 * verifies its size and MIME type, and opens it using Android's native PDF viewer or Photo Viewer Intent.
 */
export async function openPdfWithNativeViewer(options: OpenPdfOptions): Promise<OpenPdfResult> {
  const { url, storagePath, bucket, noteId, fileName, mimeType, fileType, onProgress } = options;

  const updateProgress = (percent: number, text: string) => {
    if (onProgress) onProgress(percent, text);
  };

  const isImg = isImageFile(fileName, url || storagePath, mimeType, fileType);
  const ext = getFileExtension(fileName || storagePath || url || "", isImg);
  const contentType = getMimeType(fileName || url || "", mimeType, isImg);

  updateProgress(5, "Please wait...");

  if (!url && !storagePath) {
    throw new Error(isImg ? "Missing photo file location or URL." : "Missing PDF file location or URL.");
  }

  const activeBucket = getBucketName(bucket);
  const activePath = sanitizeStoragePath(storagePath || url, activeBucket);
  const cacheFileName = getPdfCacheFileName(activePath || url, noteId, isImg, ext);

  const isNative = Capacitor.isNativePlatform();

  // Step 1: Check existing cache in Directory.Cache
  if (isNative) {
    try {
      updateProgress(10, "Please wait...");
      const statResult = await Filesystem.stat({
        path: cacheFileName,
        directory: Directory.Cache
      });

      if (statResult && statResult.size > 0) {
        console.log(`[NativePdfService] Found existing cached file "${cacheFileName}" (${statResult.size} bytes).`);
        updateProgress(80, "Opening notes...");

        const uriResult = await Filesystem.getUri({
          path: cacheFileName,
          directory: Directory.Cache
        });

        updateProgress(95, "Opening notes...");

        try {
          await FileOpener.open({
            filePath: uriResult.uri,
            contentType: contentType,
            openWithDefault: false
          });

          updateProgress(100, isImg ? "Photo opened successfully" : "PDF opened successfully");
          return { success: true, cachedPath: uriResult.uri, isNative: true };
        } catch (openerErr: any) {
          const errStr = String(openerErr?.message || openerErr).toLowerCase();
          console.warn("[NativePdfService] Cached file opener error:", openerErr);

          if (
            errStr.includes("no app") ||
            errStr.includes("activitynotfound") ||
            errStr.includes("not found") ||
            errStr.includes("no handler") ||
            errStr.includes("cannot open")
          ) {
            throw new Error(isImg ? "No photo viewer app installed on this device." : "No PDF reader installed on this device.");
          }

          console.log("[NativePdfService] Removing invalid or unreadable cached file...");
          try {
            await Filesystem.deleteFile({ path: cacheFileName, directory: Directory.Cache });
          } catch {
            // ignore cleanup errors
          }
        }
      }
    } catch (cacheStatErr) {
      console.log("[NativePdfService] Cache miss or stat error, downloading:", cacheStatErr);
    }
  }

  // Step 2: Download file
  updateProgress(20, "Please wait...");
  let downloadUrl = "";
  try {
    downloadUrl = await getPdfDownloadUrl(url, activeBucket);
  } catch (resErr: any) {
    console.warn("[NativePdfService] getPdfDownloadUrl failed, trying fallback:", resErr);
  }

  let pdfBlob: Blob | null = null;

  // 2a. Direct download
  if (activePath && !url.startsWith("data:") && !url.startsWith("blob:")) {
    try {
      updateProgress(35, "Downloading...");
      const { data: sdkData, error: sdkErr } = await supabase.storage.from(activeBucket).download(activePath);

      if (!sdkErr && sdkData && sdkData.size > 0) {
        pdfBlob = sdkData;
      } else {
        const { data: signedData, error: signedErr } = await supabase.storage
          .from(activeBucket)
          .createSignedUrl(activePath, 3600);

        if (!signedErr && signedData?.signedUrl) {
          downloadUrl = signedData.signedUrl;
        }
      }
    } catch (sdkEx) {
      console.warn("[NativePdfService] Direct SDK download exception:", sdkEx);
    }
  }

  // 2b. Fetch via HTTPS downloadUrl if blob not retrieved yet
  if (!pdfBlob) {
    if (!downloadUrl) {
      throw new Error(isImg ? "Unable to resolve photo storage URL or signed link." : "Unable to resolve PDF storage URL or signed link.");
    }

    updateProgress(50, "Downloading...");

    if (downloadUrl.startsWith("data:") || downloadUrl.startsWith("JVBERi")) {
      pdfBlob = await dataUrlToBlob(downloadUrl);
    } else {
      const response = await fetch(downloadUrl);
      if (!response.ok) {
        if (response.status === 404) {
          throw new Error("File not found.");
        } else if (response.status === 401 || response.status === 403) {
          throw new Error("Access denied.");
        }
        throw new Error("Unable to download file.");
      }
      pdfBlob = await response.blob();
    }
  }

  // Step 3: Verification
  updateProgress(75, "Please wait...");

  if (!pdfBlob || pdfBlob.size <= 0) {
    throw new Error("Downloaded file is empty.");
  }

  if (!isImg) {
    const isValidHeader = await validatePdfHeader(pdfBlob);
    if (!isValidHeader) {
      throw new Error("Invalid document format.");
    }
  }

  // Step 4: Write to Cache Directory if on Native Android
  if (isNative) {
    updateProgress(85, "Saving...");
    const base64Data = await blobToBase64(pdfBlob);

    await Filesystem.writeFile({
      path: cacheFileName,
      data: base64Data,
      directory: Directory.Cache,
      recursive: true
    });

    const cachedStat = await Filesystem.stat({
      path: cacheFileName,
      directory: Directory.Cache
    });

    if (!cachedStat || cachedStat.size <= 0) {
      throw new Error("Failed to save file.");
    }

    const uriResult = await Filesystem.getUri({
      path: cacheFileName,
      directory: Directory.Cache
    });

    updateProgress(95, "Opening notes...");

    try {
      await FileOpener.open({
        filePath: uriResult.uri,
        contentType: contentType,
        openWithDefault: false
      });

      updateProgress(100, "Opened successfully");
      return { success: true, cachedPath: uriResult.uri, isNative: true };
    } catch (openErr: any) {
      const errStr = String(openErr?.message || openErr).toLowerCase();
      console.error("[NativePdfService] FileOpener failed:", openErr);

      if (
        errStr.includes("no app") ||
        errStr.includes("activitynotfound") ||
        errStr.includes("not found") ||
        errStr.includes("no handler") ||
        errStr.includes("cannot open")
      ) {
        throw new Error(isImg ? "No photo viewer app installed on this device." : "No PDF reader installed on this device.");
      }

      throw new Error("Failed to open file viewer.");
    }
  } else {
    // Web / Browser Preview Fallback
    updateProgress(95, "Opening notes...");
    const blobObjectUrl = URL.createObjectURL(pdfBlob);
    window.open(blobObjectUrl || downloadUrl, "_blank");
    updateProgress(100, "Opened successfully");
    return { success: true, isNative: false };
  }
}

/**
 * Saves and opens a client-side generated PDF blob on native Android or web.
 */
export async function saveAndOpenGeneratedPdf(pdfBlob: Blob, fileName: string): Promise<void> {
  const isNative = typeof Capacitor !== "undefined" && Capacitor.isNativePlatform();
  if (isNative) {
    const base64Data = await blobToBase64(pdfBlob);
    await Filesystem.writeFile({
      path: fileName,
      data: base64Data,
      directory: Directory.Cache,
      recursive: true
    });
    const uriResult = await Filesystem.getUri({
      path: fileName,
      directory: Directory.Cache
    });
    await FileOpener.open({
      filePath: uriResult.uri,
      contentType: "application/pdf",
      openWithDefault: false
    });
  } else {
    const url = URL.createObjectURL(pdfBlob);
    const a = document.createElement("a");
    a.href = url;
    a.target = "_blank";
    a.download = fileName;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    setTimeout(() => URL.revokeObjectURL(url), 10000);
  }
}
