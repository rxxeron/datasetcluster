/**
 * ============================================================================
 * PREP404 Datacenter / AAPathshala Central Cloud Tracker & Google Drive Sync
 * ============================================================================
 * 
 * Features:
 * 1. Automatically mirrors local disk folder hierarchy in Google Drive:
 *    Root: "aapathshala_parsed"
 *    ├── admission_question_bank/
 *    │   └── <book_slug>/
 *    │       └── book.json
 *    ├── main_book_11_12/
 *    │   └── <book_slug>/
 *    │       └── book.json
 *    └── main_book_solution/
 *        └── <book_slug>/
 *            └── book.json
 * 
 * 2. Saves uploaded book.json directly into its exact Google Drive book folder.
 * 3. Updates Google Sheet with clickable Drive link (COL_OUTPUT_JSON) and COMPLETED status.
 * 4. Multi-machine atomic claiming & real-time synchronization.
 * 5. Provides custom spreadsheet menu "🛰️ PREP404 Datacenter" to create all folders in 1 click!
 */

const SHEET_NAME = "Tracker";
const ROOT_DRIVE_FOLDER_NAME = "aapathshala_parsed";
const STALE_LOCK_HOURS = 3;

// Column Indexes (1-based for Google Sheets)
const COL_DRIVE_ID = 1;      // A
const COL_COLLECTION = 2;    // B
const COL_SUBJECT = 3;       // C
const COL_EDITION = 4;       // D
const COL_FILENAME = 5;      // E
const COL_STATUS = 6;        // F
const COL_WORKER = 7;        // G
const COL_TOTAL_PAGES = 8;   // H
const COL_COMPL_PAGES = 9;   // I
const COL_CLAIMED_AT = 10;   // J
const COL_COMPL_AT = 11;     // K
const COL_OUTPUT_JSON = 12;  // L (Stores Google Drive URL)
const COL_ERROR = 13;        // M

/**
 * Custom Menu in Google Sheets UI
 */
function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu("🛰️ PREP404 Datacenter")
    .addItem("📁 Initialize Google Drive Folder Tree", "menuCreateAllFolders")
    .addItem("📊 Refresh Tracker Stats", "menuRefreshStats")
    .addToUi();
}

function getSheet() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  return ss.getSheetByName(SHEET_NAME) || ss.getSheets()[0];
}

function jsonResponse(obj) {
  return ContentService.createTextOutput(JSON.stringify(obj))
    .setMimeType(ContentService.MimeType.JSON);
}

// ============================================================================
// Google Drive Folder Hierarchy Helpers
// ============================================================================

/**
 * Gets or creates the root folder in Google Drive.
 */
function getRootFolder() {
  const props = PropertiesService.getScriptProperties();
  const cachedId = props.getProperty("ROOT_FOLDER_ID");
  if (cachedId) {
    try {
      return DriveApp.getFolderById(cachedId);
    } catch (e) {
      // ID invalid or deleted, recreate below
    }
  }

  const folders = DriveApp.getFoldersByName(ROOT_DRIVE_FOLDER_NAME);
  let root;
  if (folders.hasNext()) {
    root = folders.next();
  } else {
    root = DriveApp.createFolder(ROOT_DRIVE_FOLDER_NAME);
    root.setDescription("PREP404 / AAPathshala Verified Parsed Textbooks & Solutions Repository");
  }
  props.setProperty("ROOT_FOLDER_ID", root.getId());
  return root;
}

/**
 * Gets or creates a subfolder by name within a parent folder.
 */
function getOrCreateSubfolder(parentFolder, name) {
  const cleanName = String(name || "unspecified").trim();
  const folders = parentFolder.getFoldersByName(cleanName);
  if (folders.hasNext()) {
    return folders.next();
  }
  return parentFolder.createFolder(cleanName);
}

/**
 * Navigates or creates the exact hierarchy:
 * Root -> <collection> -> <book_slug>
 */
function getBookFolder(collection, bookSlug) {
  const root = getRootFolder();
  const colFolder = getOrCreateSubfolder(root, collection || "unspecified");
  const bookFolder = getOrCreateSubfolder(colFolder, bookSlug || "unknown_book");
  return bookFolder;
}

/**
 * Saves or updates book.json inside the book's Google Drive folder.
 */
function saveBookJsonToDrive(collection, bookSlug, jsonContent) {
  const folder = getBookFolder(collection, bookSlug);
  const fileName = "book.json";
  const files = folder.getFilesByName(fileName);
  
  let file;
  if (files.hasNext()) {
    file = files.next();
    file.setContent(jsonContent);
  } else {
    file = folder.createFile(fileName, jsonContent, MimeType.PLAIN_TEXT);
  }
  
  file.setDescription(`Verified book.json for ${bookSlug} (${collection})`);
  return {
    fileId: file.getId(),
    fileUrl: file.getUrl(),
    folderId: folder.getId(),
    folderUrl: folder.getUrl()
  };
}

// ============================================================================
// HTTP GET / POST Endpoints
// ============================================================================

function doGet(e) {
  try {
    const params = (e && e.parameter) ? e.parameter : {};
    const action = (params.action || "").toLowerCase();

    if (action === "ping") {
      const root = getRootFolder();
      return jsonResponse({
        success: true,
        message: "PREP404 Datacenter Cloud Tracker & Drive Sync Online",
        rootFolderUrl: root.getUrl(),
        time: new Date().toISOString()
      });
    }
    if (action === "dump") {
      return jsonResponse({ success: true, data: dumpAllStatuses() });
    }
    if (action === "check") {
      return jsonResponse(checkFileStatus(params.id));
    }
    if (action === "claim") {
      return jsonResponse(claimFile(params.id, params.worker));
    }
    if (action === "complete") {
      return jsonResponse(completeFile(params.id, params.worker, params.total_pages, params.completed_pages, params.output_json));
    }
    if (action === "fail") {
      return jsonResponse(failFile(params.id, params.worker, params.error));
    }

    return jsonResponse({
      success: true,
      message: "Ready. Supported actions: ping, dump, check, claim, complete, fail, upload_book, bulk_init, create_folders"
    });
  } catch (err) {
    return jsonResponse({ success: false, error: err.toString() });
  }
}

function doPost(e) {
  try {
    let payload = {};
    if (e && e.postData && e.postData.contents) {
      try {
        payload = JSON.parse(e.postData.contents);
      } catch (ex) {
        payload = e.parameter || {};
      }
    } else if (e && e.parameter) {
      payload = e.parameter;
    }

    const action = (payload.action || "").toLowerCase();

    // 1. Upload Book JSON directly to Google Drive folder & update sheet
    if (action === "upload_book") {
      return jsonResponse(handleUploadBook(payload));
    }

    // 2. Standard Claim
    if (action === "claim") {
      return jsonResponse(claimFile(payload.id, payload.worker));
    }

    // 3. Mark Complete
    if (action === "complete") {
      return jsonResponse(completeFile(payload.id, payload.worker, payload.total_pages, payload.completed_pages, payload.output_json));
    }

    // 4. Mark Failed
    if (action === "fail") {
      return jsonResponse(failFile(payload.id, payload.worker, payload.error));
    }

    // 5. Batch create folders for an array of items
    if (action === "create_folders" && payload.items) {
      return jsonResponse(batchCreateFolders(payload.items));
    }

    // 6. Bulk init sheet items
    if (action === "bulk_init" && payload.items) {
      return jsonResponse(bulkInitSheet(payload.items));
    }

    return doGet(e);
  } catch (err) {
    return jsonResponse({ success: false, error: err.toString() });
  }
}

/**
 * Handles incoming book.json upload from workers.
 */
function handleUploadBook(payload) {
  const driveId = payload.id;
  const collection = payload.collection || "unspecified";
  const bookSlug = payload.book_slug || driveId || "unknown";
  const content = payload.content;
  const totalPages = payload.total_pages || 0;
  const completedPages = payload.completed_pages || totalPages;
  const worker = payload.worker || "Cloud-Runner";

  if (!content) {
    return { success: false, reason: "missing_content" };
  }

  // 1. Save directly to Google Drive hierarchy
  const driveRes = saveBookJsonToDrive(collection, bookSlug, content);

  // 2. Update Google Sheet Tracker
  if (driveId) {
    completeFile(driveId, worker, totalPages, completedPages, driveRes.fileUrl);
  }

  return {
    success: true,
    id: driveId,
    collection: collection,
    book_slug: bookSlug,
    drive_file_id: driveRes.fileId,
    drive_file_url: driveRes.fileUrl,
    drive_folder_url: driveRes.folderUrl
  };
}

// ============================================================================
// Sheet Tracker Helpers & Atomic Locking
// ============================================================================

function findRowIndex(sheet, targetId) {
  if (!targetId) return -1;
  const lastRow = sheet.getLastRow();
  if (lastRow < 2) return -1;
  const data = sheet.getRange(2, COL_DRIVE_ID, lastRow - 1, 1).getValues();
  for (let i = 0; i < data.length; i++) {
    if (String(data[i][0]).trim() === String(targetId).trim()) {
      return i + 2;
    }
  }
  return -1;
}

function dumpAllStatuses() {
  const sheet = getSheet();
  const lastRow = sheet.getLastRow();
  if (lastRow < 2) return {};

  const range = sheet.getRange(2, 1, lastRow - 1, COL_ERROR).getValues();
  const result = {};

  for (let i = 0; i < range.length; i++) {
    const row = range[i];
    const id = String(row[COL_DRIVE_ID - 1]).trim();
    if (!id) continue;

    result[id] = {
      status: String(row[COL_STATUS - 1] || "PENDING").toUpperCase(),
      worker: String(row[COL_WORKER - 1] || ""),
      total_pages: Number(row[COL_TOTAL_PAGES - 1]) || 0,
      completed_pages: Number(row[COL_COMPL_PAGES - 1]) || 0,
      claimed_at: String(row[COL_CLAIMED_AT - 1] || ""),
      completed_at: String(row[COL_COMPL_AT - 1] || ""),
      drive_url: String(row[COL_OUTPUT_JSON - 1] || ""),
      error: String(row[COL_ERROR - 1] || "")
    };
  }
  return result;
}

function checkFileStatus(driveId) {
  const sheet = getSheet();
  const rowIdx = findRowIndex(sheet, driveId);
  if (rowIdx === -1) return { success: false, reason: "not_found", id: driveId };

  const row = sheet.getRange(rowIdx, 1, 1, COL_ERROR).getValues()[0];
  return {
    success: true,
    id: driveId,
    filename: row[COL_FILENAME - 1],
    status: String(row[COL_STATUS - 1] || "PENDING").toUpperCase(),
    worker: row[COL_WORKER - 1],
    claimed_at: row[COL_CLAIMED_AT - 1],
    completed_at: row[COL_COMPL_AT - 1],
    drive_url: row[COL_OUTPUT_JSON - 1]
  };
}

function claimFile(driveId, workerName) {
  if (!driveId) return { success: false, reason: "missing_id" };
  const worker = workerName || "Unknown-Worker";
  const sheet = getSheet();
  const rowIdx = findRowIndex(sheet, driveId);
  if (rowIdx === -1) return { success: false, reason: "id_not_found_in_sheet", id: driveId };

  const lock = LockService.getScriptLock();
  try {
    lock.waitLock(10000);
    const currentStatus = String(sheet.getRange(rowIdx, COL_STATUS).getValue() || "PENDING").toUpperCase();
    const currentWorker = String(sheet.getRange(rowIdx, COL_WORKER).getValue() || "");
    const claimedAtStr = String(sheet.getRange(rowIdx, COL_CLAIMED_AT).getValue() || "");

    if (currentStatus === "COMPLETED") {
      return { success: false, reason: "already_completed", id: driveId, worker: currentWorker };
    }

    if (currentStatus === "IN_PROGRESS" && currentWorker !== worker) {
      if (claimedAtStr) {
        const claimDate = new Date(claimedAtStr);
        const hoursAgo = (new Date() - claimDate) / (1000 * 60 * 60);
        if (hoursAgo < STALE_LOCK_HOURS) {
          return {
            success: false,
            reason: "claimed_by_other_worker",
            worker: currentWorker,
            claimed_at: claimedAtStr,
            id: driveId
          };
        }
      }
    }

    const nowIso = new Date().toISOString();
    sheet.getRange(rowIdx, COL_STATUS).setValue("IN_PROGRESS").setBackground("#FFF2CC");
    sheet.getRange(rowIdx, COL_WORKER).setValue(worker);
    sheet.getRange(rowIdx, COL_CLAIMED_AT).setValue(nowIso);

    return {
      success: true,
      id: driveId,
      status: "IN_PROGRESS",
      worker: worker,
      claimed_at: nowIso
    };
  } finally {
    lock.releaseLock();
  }
}

function completeFile(driveId, workerName, totalPages, completedPages, outputJson) {
  if (!driveId) return { success: false, reason: "missing_id" };
  const sheet = getSheet();
  const rowIdx = findRowIndex(sheet, driveId);
  if (rowIdx === -1) return { success: false, reason: "id_not_found" };

  const nowIso = new Date().toISOString();
  sheet.getRange(rowIdx, COL_STATUS).setValue("COMPLETED").setBackground("#D9EAD3");
  if (workerName) sheet.getRange(rowIdx, COL_WORKER).setValue(workerName);
  if (totalPages) sheet.getRange(rowIdx, COL_TOTAL_PAGES).setValue(Number(totalPages));
  if (completedPages) sheet.getRange(rowIdx, COL_COMPL_PAGES).setValue(Number(completedPages));
  
  if (outputJson) {
    if (outputJson.startsWith("http")) {
      sheet.getRange(rowIdx, COL_OUTPUT_JSON).setFormula(`=HYPERLINK("${outputJson}", "Open book.json")`);
    } else {
      sheet.getRange(rowIdx, COL_OUTPUT_JSON).setValue(outputJson);
    }
  }

  sheet.getRange(rowIdx, COL_COMPL_AT).setValue(nowIso);
  sheet.getRange(rowIdx, COL_ERROR).setValue("");

  return { success: true, id: driveId, status: "COMPLETED", completed_at: nowIso, drive_url: outputJson };
}

function failFile(driveId, workerName, errorMsg) {
  if (!driveId) return { success: false, reason: "missing_id" };
  const sheet = getSheet();
  const rowIdx = findRowIndex(sheet, driveId);
  if (rowIdx === -1) return { success: false, reason: "id_not_found" };

  sheet.getRange(rowIdx, COL_STATUS).setValue("FAILED").setBackground("#FCE5CD");
  if (workerName) sheet.getRange(rowIdx, COL_WORKER).setValue(workerName);
  sheet.getRange(rowIdx, COL_ERROR).setValue(errorMsg || "Unknown error");

  return { success: true, id: driveId, status: "FAILED" };
}

// ============================================================================
// Batch Folder Initializer
// ============================================================================

function batchCreateFolders(items) {
  const root = getRootFolder();
  const colCache = {};
  let createdCount = 0;

  for (let i = 0; i < items.length; i++) {
    const item = items[i];
    const col = item.collection || "unspecified";
    const slug = item.book_slug || item.id || `book_${i}`;

    if (!colCache[col]) {
      colCache[col] = getOrCreateSubfolder(root, col);
    }
    const colFolder = colCache[col];
    getOrCreateSubfolder(colFolder, slug);
    createdCount++;
  }

  return {
    success: true,
    rootFolderUrl: root.getUrl(),
    foldersProcessed: createdCount
  };
}

function menuCreateAllFolders() {
  const sheet = getSheet();
  const lastRow = sheet.getLastRow();
  if (lastRow < 2) {
    SpreadsheetApp.getUi().alert("No items found in sheet to build folders.");
    return;
  }

  const data = sheet.getRange(2, 1, lastRow - 1, COL_FILENAME).getValues();
  const items = [];
  for (let i = 0; i < data.length; i++) {
    const driveId = data[i][COL_DRIVE_ID - 1];
    const col = data[i][COL_COLLECTION - 1];
    const fname = data[i][COL_FILENAME - 1];
    if (driveId && col) {
      items.push({
        id: driveId,
        collection: col,
        book_slug: fname.replace(/\.pdf$/i, "") + "_" + driveId.slice(0, 8)
      });
    }
  }

  SpreadsheetApp.getActiveSpreadsheet().toast(`Creating Google Drive folder tree for ${items.length} books...`, "PREP404 Datacenter", 5);
  const res = batchCreateFolders(items);
  SpreadsheetApp.getUi().alert(`✅ Successfully initialized Google Drive folder hierarchy!\n\nRoot Folder URL: ${res.rootFolderUrl}`);
}
