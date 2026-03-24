package com.example.photocleaner

import android.Manifest
import android.app.DownloadManager
import android.app.PendingIntent
import android.content.BroadcastReceiver
import android.content.ContentUris
import android.content.Context
import android.content.Intent
import android.content.IntentFilter
import android.content.pm.PackageManager
import android.graphics.Bitmap
import android.graphics.Color
import android.graphics.ImageDecoder
import android.graphics.drawable.GradientDrawable
import android.net.Uri
import android.os.Bundle
import android.os.Environment
import android.provider.MediaStore
import android.provider.Settings
import android.animation.ObjectAnimator
import android.util.LruCache
import android.view.Gravity
import android.view.MotionEvent
import android.view.View
import android.view.ViewGroup
import android.view.animation.AccelerateDecelerateInterpolator
import android.view.animation.LinearInterpolator
import android.widget.FrameLayout
import android.widget.GridLayout
import android.widget.ImageButton
import android.widget.ImageView
import android.widget.LinearLayout
import android.widget.ProgressBar
import android.widget.TextView
import android.widget.Toast
import androidx.activity.enableEdgeToEdge
import androidx.activity.OnBackPressedCallback
import androidx.activity.result.IntentSenderRequest
import androidx.activity.result.contract.ActivityResultContracts
import androidx.appcompat.app.AlertDialog
import androidx.appcompat.app.AppCompatActivity
import androidx.appcompat.app.AppCompatDelegate
import androidx.core.content.ContextCompat
import androidx.core.content.FileProvider
import androidx.core.view.ViewCompat
import androidx.core.view.WindowInsetsCompat
import androidx.core.view.isVisible
import com.google.android.material.button.MaterialButton
import org.json.JSONArray
import org.json.JSONObject
import java.io.File
import java.text.DecimalFormat
import java.text.SimpleDateFormat
import java.net.HttpURLConnection
import java.net.URL
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneId
import java.util.ArrayDeque
import java.util.Date
import java.util.Locale
import java.util.concurrent.Executors
import kotlin.math.abs
import kotlin.math.max
import kotlin.math.min
import kotlin.random.Random

class MainActivity : AppCompatActivity() {

    private data class PhotoItem(
        val uri: Uri,
        val name: String,
        val dateTakenMillis: Long,
        val sizeBytes: Long,
        val width: Int,
        val height: Int,
        val bucketName: String,
        val relativePath: String
    )

    private data class ReviewStats(
        var keptCount: Int = 0,
        var markedDeleteCount: Int = 0,
        var skippedCount: Int = 0,
        var releasableBytes: Long = 0L
    )

    private data class ActionRecord(
        val photo: PhotoItem,
        val action: ActionType
    )

    private data class SmartInsights(
        val screenshots: List<PhotoItem> = emptyList(),
        val similar: List<PhotoItem> = emptyList(),
        val blur: List<PhotoItem> = emptyList()
    )

    private data class HistoryRecord(
        val timestamp: Long,
        val title: String,
        val meta: String,
        val freedBytes: Long,
        val icon: String,
        val accent: String
    )

    private data class DeleteRequest(
        val uris: List<Uri>,
        val freedBytes: Long,
        val deletedCount: Int,
        val historyRecord: HistoryRecord,
        val smartModeToReload: SmartMode? = null
    )

    private data class UpdateInfo(
        val versionLabel: String,
        val releaseTitle: String,
        val notes: String,
        val htmlUrl: String,
        val downloadUrl: String?,
        val assetName: String?
    )

    private enum class ActionType {
        KEEP,
        MARK_DELETE,
        SKIP
    }

    private enum class SmartMode(
        val title: String,
        val icon: String,
        val accent: String
    ) {
        SCREENSHOT("截图清理", "🖼️", "amber"),
        SIMILAR("相似照片", "🔁", "blue"),
        BLUR("模糊照片", "🌫️", "purple")
    }

    private val worker = Executors.newSingleThreadExecutor()
    private val imageWorker = Executors.newSingleThreadExecutor()
    private val smartThumbWorker = Executors.newFixedThreadPool(3)
    private val updateWorker = Executors.newSingleThreadExecutor()
    private val random = Random(System.currentTimeMillis())
    private val decimalFormat = DecimalFormat("0.#")
    private val dateFormatter = SimpleDateFormat("yyyy.MM.dd", Locale.getDefault())
    private val entryTimeFormatter = SimpleDateFormat("M月d日 HH:mm", Locale.getDefault())
    private val zoneId: ZoneId = ZoneId.systemDefault()
    private val smartThumbCache = object : LruCache<String, Bitmap>(24 * 1024) {
        override fun sizeOf(key: String, value: Bitmap): Int = value.byteCount / 1024
    }

    private lateinit var welcomeContainer: View
    private lateinit var smartContainer: View
    private lateinit var viewerContainer: View
    private lateinit var resultContainer: View
    private lateinit var historyContainer: View

    private lateinit var screenshotCard: View
    private lateinit var similarCard: View
    private lateinit var blurCard: View
    private lateinit var screenshotMetaText: TextView
    private lateinit var similarMetaText: TextView
    private lateinit var blurMetaText: TextView
    private lateinit var statTotalPhotosText: TextView
    private lateinit var statReleasableText: TextView
    private lateinit var statEstimatedTimeText: TextView
    private lateinit var deletedTotalText: TextView
    private lateinit var permissionHintText: TextView
    private lateinit var startButton: MaterialButton
    private lateinit var startButtonShimmer: View
    private lateinit var historyButton: MaterialButton
    private lateinit var checkUpdateButton: MaterialButton
    private lateinit var themeToggleButton: MaterialButton

    private lateinit var smartBackButton: ImageButton
    private lateinit var smartTitleText: TextView
    private lateinit var smartSubtitleText: TextView
    private lateinit var smartSelectedText: TextView
    private lateinit var smartGrid: GridLayout
    private lateinit var smartEmptyText: TextView
    private lateinit var smartCountText: TextView
    private lateinit var smartSizeText: TextView
    private lateinit var smartDeleteButton: MaterialButton
    private lateinit var smartSelectAllButton: MaterialButton

    private lateinit var viewerBackButton: ImageButton
    private lateinit var viewerCounterText: TextView
    private lateinit var photoCard: View
    private lateinit var photoImageView: ImageView
    private lateinit var swipeKeepOverlay: View
    private lateinit var swipeDeleteOverlay: View
    private lateinit var loadingText: TextView
    private lateinit var swipeStatusText: TextView
    private lateinit var photoDateText: TextView
    private lateinit var photoNameText: TextView
    private lateinit var photoMetaText: TextView
    private lateinit var photoTagsContainer: LinearLayout
    private lateinit var swipeGuideText: TextView
    private lateinit var undoButton: ImageView
    private lateinit var deleteButton: ImageView
    private lateinit var keepButton: ImageView
    private lateinit var skipButton: ImageView
    private lateinit var progressBar: ProgressBar
    private lateinit var progressDoneText: TextView
    private lateinit var progressPercentText: TextView

    private lateinit var resultKeptText: TextView
    private lateinit var resultDeletedText: TextView
    private lateinit var resultSkippedText: TextView
    private lateinit var resultFreedText: TextView
    private lateinit var resultHintText: TextView
    private lateinit var resultBackButton: ImageButton
    private lateinit var resultDeleteButton: MaterialButton
    private lateinit var memoryButton: MaterialButton
    private lateinit var nextBatchButton: MaterialButton

    private lateinit var historyBackButton: ImageButton
    private lateinit var historyFreedTotalText: TextView
    private lateinit var historyStreakContainer: LinearLayout
    private lateinit var badgeContainer: LinearLayout
    private lateinit var historyEntryContainer: LinearLayout

    private val allPhotos = mutableListOf<PhotoItem>()
    private var smartInsights = SmartInsights()
    private val smartSelectedUris = linkedSetOf<Uri>()
    private var currentSmartMode: SmartMode? = null
    private var currentSmartItems: List<PhotoItem> = emptyList()

    private val reviewQueue = mutableListOf<PhotoItem>()
    private val pendingDeleteUris = linkedSetOf<Uri>()
    private val reviewHistory = ArrayDeque<ActionRecord>()
    private var reviewStats = ReviewStats()
    private var currentBatchSize = 0

    private var activeDeleteRequest: DeleteRequest? = null
    private var photosLoadInProgress = false
    private var imageLoadToken = 0
    private var smartAnalysisToken = 0
    private var progressAnimator: ObjectAnimator? = null
    private var currentScreen: Screen = Screen.WELCOME
    private val screenBackStack = ArrayDeque<Screen>()
    private var reviewWarmupToken = 0
    private var preparedReviewQueue: List<PhotoItem>? = null
    private var preparedFirstBitmap: Bitmap? = null
    private var preparedFirstBitmapUri: Uri? = null
    private var updateCheckInProgress = false

    private val preferences by lazy { getSharedPreferences(PREFS_NAME, MODE_PRIVATE) }
    private val historyRecords = mutableListOf<HistoryRecord>()
    private val downloadManager by lazy { getSystemService(DOWNLOAD_SERVICE) as DownloadManager }
    private var updateDownloadId: Long = -1L
    private var pendingInstallFileName: String? = null
    private val downloadCompleteReceiver = object : BroadcastReceiver() {
        override fun onReceive(context: Context?, intent: Intent?) {
            if (intent?.action != DownloadManager.ACTION_DOWNLOAD_COMPLETE) return
            val downloadId = intent.getLongExtra(DownloadManager.EXTRA_DOWNLOAD_ID, -1L)
            if (downloadId != updateDownloadId || downloadId == -1L) return
            handleUpdateDownloadComplete(downloadId)
        }
    }

    private val permissionLauncher =
        registerForActivityResult(ActivityResultContracts.RequestPermission()) { granted ->
            if (granted) {
                loadPhotos(startReviewAfter = true)
            } else {
                permissionHintText.text = "没有读取相册权限就无法开始整理，请重新授权。"
                showToast("请先允许读取相册")
            }
        }

    private val deleteLauncher =
        registerForActivityResult(ActivityResultContracts.StartIntentSenderForResult()) { result ->
            val request = activeDeleteRequest
            activeDeleteRequest = null
            if (request == null) return@registerForActivityResult

            if (result.resultCode == RESULT_OK) {
                saveDeletedTotal(readDeletedTotal() + request.deletedCount)
                saveFreedBytesTotal(readFreedBytesTotal() + request.freedBytes)
                historyRecords.add(0, request.historyRecord)
                saveHistory()
                refreshAfterDelete(request)
            } else {
                showToast("已取消删除操作")
            }
        }

    override fun onCreate(savedInstanceState: Bundle?) {
        applySavedThemeMode()
        super.onCreate(savedInstanceState)
        enableEdgeToEdge()
        setContentView(R.layout.activity_main)
        loadHistory()
        bindViews()
        setupInsets()
        setupListeners()
        setupBackNavigation()
        registerReceiver(
            downloadCompleteReceiver,
            IntentFilter(DownloadManager.ACTION_DOWNLOAD_COMPLETE),
            Context.RECEIVER_NOT_EXPORTED
        )
        startShimmerAnimation()
        updateThemeToggleText()
        updateWelcomeSummary()
        showScreen(Screen.WELCOME, addToBackStack = false)
        preloadWelcomeDataIfPermitted()
    }

    override fun onDestroy() {
        unregisterReceiver(downloadCompleteReceiver)
        worker.shutdownNow()
        imageWorker.shutdownNow()
        smartThumbWorker.shutdownNow()
        updateWorker.shutdownNow()
        super.onDestroy()
    }

    override fun onResume() {
        super.onResume()
        pendingInstallFileName?.takeIf { packageManager.canRequestPackageInstalls() }?.let {
            installDownloadedApk(it)
        }
    }

    private fun bindViews() {
        welcomeContainer = findViewById(R.id.welcomeContainer)
        smartContainer = findViewById(R.id.smartContainer)
        viewerContainer = findViewById(R.id.viewerContainer)
        resultContainer = findViewById(R.id.resultContainer)
        historyContainer = findViewById(R.id.historyContainer)

        screenshotCard = findViewById(R.id.screenshotCard)
        similarCard = findViewById(R.id.similarCard)
        blurCard = findViewById(R.id.blurCard)
        screenshotMetaText = findViewById(R.id.screenshotMetaText)
        similarMetaText = findViewById(R.id.similarMetaText)
        blurMetaText = findViewById(R.id.blurMetaText)
        statTotalPhotosText = findViewById(R.id.statTotalPhotosText)
        statReleasableText = findViewById(R.id.statReleasableText)
        statEstimatedTimeText = findViewById(R.id.statEstimatedTimeText)
        deletedTotalText = findViewById(R.id.deletedTotalText)
        permissionHintText = findViewById(R.id.permissionHintText)
        startButton = findViewById(R.id.startButton)
        startButtonShimmer = findViewById(R.id.startButtonShimmer)
        historyButton = findViewById(R.id.historyButton)
        checkUpdateButton = findViewById(R.id.checkUpdateButton)
        themeToggleButton = findViewById(R.id.themeToggleButton)

        smartBackButton = findViewById(R.id.smartBackButton)
        smartTitleText = findViewById(R.id.smartTitleText)
        smartSubtitleText = findViewById(R.id.smartSubtitleText)
        smartSelectedText = findViewById(R.id.smartSelectedText)
        smartGrid = findViewById(R.id.smartGrid)
        smartEmptyText = findViewById(R.id.smartEmptyText)
        smartCountText = findViewById(R.id.smartCountText)
        smartSizeText = findViewById(R.id.smartSizeText)
        smartDeleteButton = findViewById(R.id.smartDeleteButton)
        smartSelectAllButton = findViewById(R.id.smartSelectAllButton)

        viewerBackButton = findViewById(R.id.viewerBackButton)
        viewerCounterText = findViewById(R.id.viewerCounterText)
        photoCard = findViewById(R.id.photoCard)
        photoImageView = findViewById(R.id.photoImageView)
        swipeKeepOverlay = findViewById(R.id.swipeKeepOverlay)
        swipeDeleteOverlay = findViewById(R.id.swipeDeleteOverlay)
        loadingText = findViewById(R.id.loadingText)
        swipeStatusText = findViewById(R.id.swipeStatusText)
        swipeKeepOverlay.bringToFront()
        swipeDeleteOverlay.bringToFront()
        loadingText.bringToFront()
        swipeStatusText.bringToFront()
        photoDateText = findViewById(R.id.photoDateText)
        photoNameText = findViewById(R.id.photoNameText)
        photoMetaText = findViewById(R.id.photoMetaText)
        photoTagsContainer = findViewById(R.id.photoTagsContainer)
        swipeGuideText = findViewById(R.id.swipeGuideText)
        undoButton = findViewById(R.id.undoButton)
        deleteButton = findViewById(R.id.deleteButton)
        keepButton = findViewById(R.id.keepButton)
        skipButton = findViewById(R.id.skipButton)
        progressBar = findViewById(R.id.progressBar)
        progressDoneText = findViewById(R.id.progressDoneText)
        progressPercentText = findViewById(R.id.progressPercentText)

        resultKeptText = findViewById(R.id.resultKeptText)
        resultDeletedText = findViewById(R.id.resultDeletedText)
        resultSkippedText = findViewById(R.id.resultSkippedText)
        resultFreedText = findViewById(R.id.resultFreedText)
        resultHintText = findViewById(R.id.resultHintText)
        resultBackButton = findViewById(R.id.resultBackButton)
        resultDeleteButton = findViewById(R.id.resultDeleteButton)
        memoryButton = findViewById(R.id.memoryButton)
        nextBatchButton = findViewById(R.id.nextBatchButton)

        historyBackButton = findViewById(R.id.historyBackButton)
        historyFreedTotalText = findViewById(R.id.historyFreedTotalText)
        historyStreakContainer = findViewById(R.id.historyStreakContainer)
        badgeContainer = findViewById(R.id.badgeContainer)
        historyEntryContainer = findViewById(R.id.historyEntryContainer)
    }

    private fun setupInsets() {
        ViewCompat.setOnApplyWindowInsetsListener(findViewById(R.id.main)) { view, insets ->
            val bars = insets.getInsets(WindowInsetsCompat.Type.systemBars())
            view.setPadding(bars.left, bars.top, bars.right, bars.bottom)
            insets
        }
    }

    private fun setupListeners() {
        startButton.setOnClickListener { requestPermissionAndStartRandomReview() }
        checkUpdateButton.setOnClickListener { checkForUpdates(userInitiated = true) }
        themeToggleButton.setOnClickListener { toggleThemeMode() }
        historyButton.setOnClickListener {
            runCatching { updateHistoryUi() }
                .onFailure { showToast("整理记录暂时加载失败，请稍后再试") }
            navigateTo(Screen.HISTORY)
        }
        screenshotCard.setOnClickListener { openSmartMode(SmartMode.SCREENSHOT) }
        similarCard.setOnClickListener { openSmartMode(SmartMode.SIMILAR) }
        blurCard.setOnClickListener { openSmartMode(SmartMode.BLUR) }

        smartBackButton.setOnClickListener { navigateBack() }
        smartDeleteButton.setOnClickListener { requestSmartDelete() }
        smartSelectAllButton.setOnClickListener { toggleSelectAllSmartItems() }

        viewerBackButton.setOnClickListener { navigateBack() }
        keepButton.setOnClickListener { handleReviewAction(ActionType.KEEP) }
        deleteButton.setOnClickListener { handleReviewAction(ActionType.MARK_DELETE) }
        skipButton.setOnClickListener { handleReviewAction(ActionType.SKIP) }
        undoButton.setOnClickListener { undoReviewAction() }
        attachPressFeedback(undoButton)
        attachPressFeedback(deleteButton)
        attachPressFeedback(keepButton)
        attachPressFeedback(skipButton)
        attachSwipeGesture()

        resultBackButton.setOnClickListener { returnToWelcome() }
        resultDeleteButton.setOnClickListener { requestReviewDelete() }
        memoryButton.setOnClickListener {
            showToast("回忆视频功能正在准备中，会优先使用你本轮保留的照片。")
        }
        nextBatchButton.setOnClickListener { requestPermissionAndStartRandomReview() }

        historyBackButton.setOnClickListener { navigateBack() }
    }

    private fun setupBackNavigation() {
        onBackPressedDispatcher.addCallback(
            this,
            object : OnBackPressedCallback(true) {
                override fun handleOnBackPressed() {
                    if (!navigateBack()) {
                        isEnabled = false
                        onBackPressedDispatcher.onBackPressed()
                    }
                }
            }
        )
    }

    private fun preloadWelcomeDataIfPermitted() {
        if (hasReadPermission()) {
            loadPhotos(startReviewAfter = false)
        } else {
            permissionHintText.text = "开始前需要读取系统相册。删除操作会在系统弹窗中确认。"
        }
    }

    private fun requestPermissionAndStartRandomReview() {
        if (hasReadPermission()) {
            if (allPhotos.isEmpty()) {
                loadPhotos(startReviewAfter = true)
            } else {
                startRandomReview()
            }
        } else {
            permissionLauncher.launch(Manifest.permission.READ_MEDIA_IMAGES)
        }
    }

    private fun loadPhotos(startReviewAfter: Boolean) {
        if (photosLoadInProgress) return
        photosLoadInProgress = true
        startButton.isEnabled = false
        permissionHintText.text = if (startReviewAfter) "正在扫描你的系统相册…" else "正在读取相册统计信息…"
        screenshotMetaText.text = "分析中…"
        similarMetaText.text = "分析中…"
        blurMetaText.text = "分析中…"

        val analysisToken = ++smartAnalysisToken
        worker.execute {
            val photos = queryPhotos()
            runOnUiThread {
                if (analysisToken != smartAnalysisToken) return@runOnUiThread
                startButton.isEnabled = true
                applyQuickPhotoState(photos)
                permissionHintText.text = if (photos.isEmpty()) {
                    "没有找到可整理的照片，或者系统只给了部分相册访问权限。"
                } else {
                    "已读取 ${photos.size} 张照片，正在补充智能清理建议…"
                }
                if (startReviewAfter) {
                    startRandomReview()
                }
            }

            val insights = analyzeSmartInsights(photos)
            runOnUiThread {
                if (analysisToken != smartAnalysisToken) return@runOnUiThread
                photosLoadInProgress = false
                smartInsights = insights
                updateWelcomeSummary()
                updateSmartCardMeta()
                permissionHintText.text = if (photos.isEmpty()) {
                    "没有找到可整理的照片，或者系统只给了部分相册访问权限。"
                } else {
                    "已读取 ${photos.size} 张照片，可随机整理，也可按建议类型集中清理。"
                }
            }
            preloadSmartThumbnails(insights)
        }
    }

    private fun refreshAfterDelete(deleteRequest: DeleteRequest) {
        photosLoadInProgress = true
        resultDeleteButton.isEnabled = false
        smartDeleteButton.isEnabled = false
        resultHintText.text = "删除成功，正在刷新相册内容…"
        permissionHintText.text = "正在刷新相册统计信息…"
        val analysisToken = ++smartAnalysisToken
        worker.execute {
            val photos = queryPhotos()
            runOnUiThread {
                if (analysisToken != smartAnalysisToken) return@runOnUiThread
                applyQuickPhotoState(photos)
                pendingDeleteUris.clear()
                smartSelectedUris.clear()
                currentSmartItems = emptyList()
                updateHistoryUi()
                if (deleteRequest.smartModeToReload != null) {
                    openSmartMode(deleteRequest.smartModeToReload)
                } else {
                    resultHintText.text = if (allPhotos.isEmpty()) {
                        "删除完成，当前没有更多照片可以整理了。"
                    } else {
                        "已从系统相册删除标记的照片，可以继续下一批整理。"
                    }
                    updateResultUi()
                    showScreen(Screen.RESULT, addToBackStack = false)
                }
                showToast("已删除 ${deleteRequest.deletedCount} 张照片")
            }

            val insights = analyzeSmartInsights(photos)
            runOnUiThread {
                if (analysisToken != smartAnalysisToken) return@runOnUiThread
                photosLoadInProgress = false
                smartInsights = insights
                updateWelcomeSummary()
                updateSmartCardMeta()
                if (deleteRequest.smartModeToReload != null && currentSmartMode == deleteRequest.smartModeToReload) {
                    openSmartMode(deleteRequest.smartModeToReload)
                }
            }
            preloadSmartThumbnails(insights)
        }
    }

    private fun applyQuickPhotoState(photos: List<PhotoItem>) {
        allPhotos.clear()
        allPhotos.addAll(photos)
        smartInsights = SmartInsights(
            screenshots = photos.filter(::isScreenshotPhoto).take(MAX_SMART_ITEMS),
            similar = emptyList(),
            blur = emptyList()
        )
        preparedReviewQueue = null
        preparedFirstBitmap = null
        preparedFirstBitmapUri = null
        reviewWarmupToken += 1
        if (photos.isNotEmpty()) {
            scheduleReviewWarmup(photos, reviewWarmupToken)
        }
        updateWelcomeSummary()
        screenshotMetaText.text = formatSuggestionMeta(smartInsights.screenshots)
        similarMetaText.text = "分析中…"
        blurMetaText.text = "分析中…"
    }

    private fun queryPhotos(): List<PhotoItem> {
        val photos = mutableListOf<PhotoItem>()
        val projection = arrayOf(
            MediaStore.Images.Media._ID,
            MediaStore.Images.Media.DISPLAY_NAME,
            MediaStore.Images.Media.DATE_TAKEN,
            MediaStore.Images.Media.DATE_ADDED,
            MediaStore.Images.Media.SIZE,
            MediaStore.Images.Media.WIDTH,
            MediaStore.Images.Media.HEIGHT,
            MediaStore.Images.Media.BUCKET_DISPLAY_NAME,
            MediaStore.Images.Media.RELATIVE_PATH
        )

        contentResolver.query(
            MediaStore.Images.Media.EXTERNAL_CONTENT_URI,
            projection,
            null,
            null,
            "${MediaStore.Images.Media.DATE_TAKEN} DESC"
        )?.use { cursor ->
            val idColumn = cursor.getColumnIndexOrThrow(MediaStore.Images.Media._ID)
            val nameColumn = cursor.getColumnIndexOrThrow(MediaStore.Images.Media.DISPLAY_NAME)
            val dateTakenColumn = cursor.getColumnIndexOrThrow(MediaStore.Images.Media.DATE_TAKEN)
            val dateAddedColumn = cursor.getColumnIndexOrThrow(MediaStore.Images.Media.DATE_ADDED)
            val sizeColumn = cursor.getColumnIndexOrThrow(MediaStore.Images.Media.SIZE)
            val widthColumn = cursor.getColumnIndexOrThrow(MediaStore.Images.Media.WIDTH)
            val heightColumn = cursor.getColumnIndexOrThrow(MediaStore.Images.Media.HEIGHT)
            val bucketColumn = cursor.getColumnIndexOrThrow(MediaStore.Images.Media.BUCKET_DISPLAY_NAME)
            val pathColumn = cursor.getColumnIndexOrThrow(MediaStore.Images.Media.RELATIVE_PATH)

            while (cursor.moveToNext()) {
                val id = cursor.getLong(idColumn)
                val dateTaken = cursor.getLong(dateTakenColumn).takeIf { it > 0 }
                    ?: cursor.getLong(dateAddedColumn) * 1000L
                photos.add(
                    PhotoItem(
                        uri = ContentUris.withAppendedId(MediaStore.Images.Media.EXTERNAL_CONTENT_URI, id),
                        name = cursor.getString(nameColumn) ?: "未命名照片",
                        dateTakenMillis = dateTaken,
                        sizeBytes = cursor.getLong(sizeColumn),
                        width = cursor.getInt(widthColumn),
                        height = cursor.getInt(heightColumn),
                        bucketName = cursor.getString(bucketColumn) ?: "",
                        relativePath = cursor.getString(pathColumn) ?: ""
                    )
                )
            }
        }
        return photos
    }

    private fun analyzeSmartInsights(photos: List<PhotoItem>): SmartInsights {
        val screenshots = photos.filter(::isScreenshotPhoto)
        val similar = findSimilarPhotos(photos)
        val blur = findBlurPhotos(photos)
        return SmartInsights(
            screenshots = screenshots.take(MAX_SMART_ITEMS),
            similar = similar.take(MAX_SMART_ITEMS),
            blur = blur.take(MAX_SMART_ITEMS)
        )
    }

    private fun findSimilarPhotos(photos: List<PhotoItem>): List<PhotoItem> {
        val sorted = photos.sortedByDescending { it.dateTakenMillis }
        val duplicates = mutableListOf<PhotoItem>()
        for (index in 1 until sorted.size) {
            val current = sorted[index]
            val previous = sorted[index - 1]
            val timeClose = abs(current.dateTakenMillis - previous.dateTakenMillis) <= SIMILAR_WINDOW_MILLIS
            val aspectClose = aspectRatioDifference(current, previous) <= 0.08f
            val dimensionClose = abs(current.width - previous.width) <= 300 && abs(current.height - previous.height) <= 300
            val sameBucket = current.bucketName == previous.bucketName
            if (timeClose && sameBucket && (aspectClose || dimensionClose)) {
                duplicates.add(current)
            }
        }
        return duplicates.distinctBy { it.uri }
    }

    private fun findBlurPhotos(photos: List<PhotoItem>): List<PhotoItem> {
        val candidates = photos
            .filterNot(::isScreenshotPhoto)
            .sortedBy { blurPriority(it) }
            .take(MAX_BLUR_ANALYSIS)

        val blurred = mutableListOf<PhotoItem>()
        candidates.forEach { photo ->
            val bitmap = loadScaledBitmap(photo.uri, 120) ?: return@forEach
            if (estimateSharpness(bitmap) < BLUR_THRESHOLD) {
                blurred.add(photo)
            }
        }
        return blurred.distinctBy { it.uri }
    }

    private fun blurPriority(photo: PhotoItem): Double {
        val pixels = max(1.0, photo.width.toDouble() * photo.height.toDouble())
        return photo.sizeBytes / pixels
    }

    private fun estimateSharpness(bitmap: Bitmap): Double {
        val width = bitmap.width
        val height = bitmap.height
        if (width < 3 || height < 3) return 0.0
        var sum = 0.0
        var sumSquares = 0.0
        var count = 0
        val stepX = max(1, width / 24)
        val stepY = max(1, height / 24)

        for (y in stepY until height - stepY step stepY) {
            for (x in stepX until width - stepX step stepX) {
                val center = luma(bitmap.getPixel(x, y))
                val left = luma(bitmap.getPixel(x - stepX, y))
                val right = luma(bitmap.getPixel(x + stepX, y))
                val up = luma(bitmap.getPixel(x, y - stepY))
                val down = luma(bitmap.getPixel(x, y + stepY))
                val laplacian = (4 * center - left - right - up - down).toDouble()
                sum += laplacian
                sumSquares += laplacian * laplacian
                count++
            }
        }
        if (count == 0) return 0.0
        val mean = sum / count
        return (sumSquares / count) - (mean * mean)
    }

    private fun luma(color: Int): Int {
        return (Color.red(color) * 299 + Color.green(color) * 587 + Color.blue(color) * 114) / 1000
    }

    private fun startRandomReview() {
        if (allPhotos.isEmpty()) {
            showToast("还没有可整理的照片")
            showScreen(Screen.WELCOME, addToBackStack = false)
            return
        }

        reviewStats = ReviewStats()
        reviewHistory.clear()
        reviewQueue.clear()
        pendingDeleteUris.clear()
        val preparedQueue = preparedReviewQueue
        if (preparedQueue != null && preparedQueue.isNotEmpty()) {
            reviewQueue.addAll(preparedQueue)
            currentBatchSize = preparedQueue.size
        } else {
            val queue = buildReviewQueue(allPhotos)
            reviewQueue.addAll(queue)
            currentBatchSize = queue.size
        }

        navigateTo(Screen.VIEWER)
        updateReviewProgress()
        bindCurrentReviewPhoto()
        swipeGuideText.alpha = 1f
        swipeGuideText.isVisible = true
    }

    private fun buildReviewQueue(source: List<PhotoItem>): List<PhotoItem> {
        return source.shuffled(random).take(min(MAX_REVIEW_BATCH, source.size))
    }

    private fun scheduleReviewWarmup(photos: List<PhotoItem>, token: Int) {
        val snapshot = photos.toList()
        imageWorker.execute {
            val queue = buildReviewQueue(snapshot)
            val firstPhoto = queue.firstOrNull()
            val firstBitmap = firstPhoto?.let { loadScaledBitmap(it.uri, 1440) }
            runOnUiThread {
                if (token != reviewWarmupToken) return@runOnUiThread
                preparedReviewQueue = queue
                preparedFirstBitmap = firstBitmap
                preparedFirstBitmapUri = firstPhoto?.uri
            }
        }
    }

    private fun bindCurrentReviewPhoto() {
        val photo = reviewQueue.firstOrNull()
        if (photo == null) {
            navigateTo(Screen.RESULT)
            updateResultUi()
            return
        }

        photoDateText.text = dateFormatter.format(Date(photo.dateTakenMillis))
        photoNameText.text = photo.name
        photoMetaText.text = buildMetaText(photo)
        renderPhotoTags(photo)
        resetSwipePreview(immediate = true)
        val preloadedBitmap = if (preparedFirstBitmapUri == photo.uri) preparedFirstBitmap else null
        loadingText.isVisible = preloadedBitmap == null
        photoImageView.setImageDrawable(null)
        photoCard.translationX = 0f
        photoCard.rotation = 0f

        if (preloadedBitmap != null) {
            photoImageView.setImageBitmap(preloadedBitmap)
            loadingText.isVisible = false
            preparedFirstBitmap = null
            preparedFirstBitmapUri = null
            preparedReviewQueue = null
            return
        }

        val token = ++imageLoadToken
        imageWorker.execute {
            val bitmap = loadScaledBitmap(photo.uri, 1440)
            runOnUiThread {
                if (token != imageLoadToken) return@runOnUiThread
                loadingText.isVisible = false
                if (bitmap != null) {
                    photoImageView.setImageBitmap(bitmap)
                } else {
                    showToast("这张照片加载失败，已自动跳过")
                    handleReviewAction(ActionType.SKIP, animate = false)
                }
            }
        }
        preparedReviewQueue = null
        preparedFirstBitmap = null
        preparedFirstBitmapUri = null
    }

    private fun handleReviewAction(action: ActionType, animate: Boolean = true) {
        val current = reviewQueue.firstOrNull() ?: return
        reviewQueue.removeAt(0)
        reviewHistory.addLast(ActionRecord(current, action))

        when (action) {
            ActionType.KEEP -> reviewStats.keptCount += 1
            ActionType.MARK_DELETE -> {
                reviewStats.markedDeleteCount += 1
                reviewStats.releasableBytes += current.sizeBytes
                pendingDeleteUris.add(current.uri)
            }
            ActionType.SKIP -> reviewStats.skippedCount += 1
        }

        if (animate) {
            showSwipeStatus(action)
            swipeGuideText.animate().alpha(0f).setDuration(220).withEndAction {
                swipeGuideText.isVisible = false
            }.start()
        }

        updateReviewProgress()
        if (reviewQueue.isEmpty()) {
            navigateTo(Screen.RESULT)
            updateResultUi()
        } else {
            bindCurrentReviewPhoto()
        }
    }

    private fun undoReviewAction() {
        if (reviewHistory.isEmpty()) {
            showToast("还没有可以撤销的操作")
            return
        }

        val last = reviewHistory.removeLast()
        when (last.action) {
            ActionType.KEEP -> reviewStats.keptCount = max(0, reviewStats.keptCount - 1)
            ActionType.MARK_DELETE -> {
                reviewStats.markedDeleteCount = max(0, reviewStats.markedDeleteCount - 1)
                reviewStats.releasableBytes = max(0L, reviewStats.releasableBytes - last.photo.sizeBytes)
                pendingDeleteUris.remove(last.photo.uri)
            }
            ActionType.SKIP -> reviewStats.skippedCount = max(0, reviewStats.skippedCount - 1)
        }
        reviewQueue.add(0, last.photo)
        navigateTo(Screen.VIEWER)
        updateReviewProgress()
        bindCurrentReviewPhoto()
        showToast("已撤销上一步")
    }

    private fun requestReviewDelete() {
        if (pendingDeleteUris.isEmpty()) {
            showToast("本轮没有待删除照片")
            return
        }
        val history = HistoryRecord(
            timestamp = System.currentTimeMillis(),
            title = "随机整理",
            meta = "保留${reviewStats.keptCount} · 删除${reviewStats.markedDeleteCount} · 跳过${reviewStats.skippedCount}",
            freedBytes = reviewStats.releasableBytes,
            icon = "📸",
            accent = "green"
        )
        requestSystemDelete(
            DeleteRequest(
                uris = pendingDeleteUris.toList(),
                freedBytes = reviewStats.releasableBytes,
                deletedCount = pendingDeleteUris.size,
                historyRecord = history
            )
        )
    }

    private fun openSmartMode(mode: SmartMode) {
        currentSmartMode = mode
        currentSmartItems = when (mode) {
            SmartMode.SCREENSHOT -> smartInsights.screenshots
            SmartMode.SIMILAR -> smartInsights.similar
            SmartMode.BLUR -> smartInsights.blur
        }
        smartSelectedUris.clear()
        smartTitleText.text = mode.title
        smartSubtitleText.text = "${currentSmartItems.size} 张候选"
        navigateTo(Screen.SMART)
        smartGrid.post {
            if (currentSmartMode != mode) return@post
            populateSmartGrid()
            updateSmartSelectionUi()
        }
    }

    private fun populateSmartGrid() {
        smartGrid.removeAllViews()
        smartEmptyText.isVisible = currentSmartItems.isEmpty()
        smartGrid.isVisible = currentSmartItems.isNotEmpty()
        if (currentSmartItems.isEmpty()) return

        val screenWidth = resources.displayMetrics.widthPixels
        val itemSize = ((screenWidth - dp(32) - dp(10)) / 3f).toInt()

        currentSmartItems.forEachIndexed { index, photo ->
            val itemView = createSmartThumbView(photo, itemSize)
            val params = GridLayout.LayoutParams().apply {
                width = itemSize
                height = itemSize
                setMargins(if (index % 3 == 0) 0 else dp(5), dp(5), 0, 0)
            }
            smartGrid.addView(itemView, params)
        }
    }

    private fun createSmartThumbView(photo: PhotoItem, size: Int): View {
        val frame = FrameLayout(this).apply {
            layoutParams = ViewGroup.LayoutParams(size, size)
            background = createSmartThumbBackground(false)
            clipToOutline = true
            setOnClickListener {
                if (smartSelectedUris.contains(photo.uri)) {
                    smartSelectedUris.remove(photo.uri)
                    background = createSmartThumbBackground(false)
                    findViewWithTag<View>("overlay")?.isVisible = false
                } else {
                    smartSelectedUris.add(photo.uri)
                    background = createSmartThumbBackground(true)
                    findViewWithTag<View>("overlay")?.isVisible = true
                }
                updateSmartSelectionUi()
            }
        }

        val image = ImageView(this).apply {
            layoutParams = FrameLayout.LayoutParams(
                ViewGroup.LayoutParams.MATCH_PARENT,
                ViewGroup.LayoutParams.MATCH_PARENT
            )
            scaleType = ImageView.ScaleType.CENTER_CROP
            setBackgroundColor(getColor(R.color.surface_3))
        }
        frame.addView(image)

        val overlay = TextView(this).apply {
            tag = "overlay"
            layoutParams = FrameLayout.LayoutParams(dp(20), dp(20), Gravity.TOP or Gravity.END).apply {
                topMargin = dp(5)
                marginEnd = dp(5)
            }
            gravity = Gravity.CENTER
            text = "✓"
            textSize = 11f
            setTextColor(getColor(R.color.white))
            background = GradientDrawable().apply {
                shape = GradientDrawable.OVAL
                setColor(getColor(R.color.accent_delete))
            }
            isVisible = false
        }
        frame.addView(overlay)

        val cachedBitmap = smartThumbCache.get(smartThumbKey(photo.uri))
        if (cachedBitmap != null) {
            image.setImageBitmap(cachedBitmap)
            return frame
        }

        smartThumbWorker.execute {
            val bitmap = loadScaledBitmap(photo.uri, 360)
            runOnUiThread {
                if (bitmap != null) {
                    smartThumbCache.put(smartThumbKey(photo.uri), bitmap)
                    image.setImageBitmap(bitmap)
                }
            }
        }
        return frame
    }

    private fun createSmartThumbBackground(selected: Boolean): GradientDrawable {
        return GradientDrawable().apply {
            cornerRadius = dp(12).toFloat()
            setColor(getColor(R.color.surface_3))
            setStroke(dp(2), getColor(if (selected) R.color.accent_delete else R.color.border))
        }
    }

    private fun updateSmartSelectionUi() {
        val count = smartSelectedUris.size
        val bytes = currentSmartItems.filter { smartSelectedUris.contains(it.uri) }.sumOf { it.sizeBytes }
        smartSelectedText.text = "$count 已选"
        smartCountText.text = "$count 张"
        smartSizeText.text = formatStorage(bytes)
        smartDeleteButton.isEnabled = count > 0
        smartDeleteButton.alpha = if (count > 0) 1f else 0.5f
    }

    private fun toggleSelectAllSmartItems() {
        if (currentSmartItems.isEmpty()) return
        if (smartSelectedUris.size == currentSmartItems.size) {
            smartSelectedUris.clear()
        } else {
            smartSelectedUris.clear()
            smartSelectedUris.addAll(currentSmartItems.map { it.uri })
        }
        populateSmartGrid()
        updateSmartSelectionUi()
    }

    private fun requestSmartDelete() {
        if (smartSelectedUris.isEmpty()) {
            showToast("请先选择要删除的照片")
            return
        }
        val mode = currentSmartMode ?: return
        val selectedPhotos = currentSmartItems.filter { smartSelectedUris.contains(it.uri) }
        val freedBytes = selectedPhotos.sumOf { it.sizeBytes }
        val history = HistoryRecord(
            timestamp = System.currentTimeMillis(),
            title = mode.title,
            meta = "智能清理 ${selectedPhotos.size} 张",
            freedBytes = freedBytes,
            icon = mode.icon,
            accent = mode.accent
        )
        requestSystemDelete(
            DeleteRequest(
                uris = smartSelectedUris.toList(),
                freedBytes = freedBytes,
                deletedCount = smartSelectedUris.size,
                historyRecord = history,
                smartModeToReload = mode
            )
        )
    }

    private fun requestSystemDelete(deleteRequest: DeleteRequest) {
        val pendingIntent = runCatching {
            MediaStore.createDeleteRequest(contentResolver, deleteRequest.uris)
        }.getOrNull()

        if (pendingIntent == null) {
            showToast("无法发起系统删除请求")
            return
        }

        activeDeleteRequest = deleteRequest
        val request = IntentSenderRequest.Builder(pendingIntent.intentSender).build()
        deleteLauncher.launch(request)
    }

    private fun renderPhotoTags(photo: PhotoItem) {
        photoTagsContainer.removeAllViews()
        val tags = mutableListOf<Pair<String, Int>>()
        if (isScreenshotPhoto(photo)) tags += "截图" to R.color.accent_warm
        if (smartInsights.similar.any { it.uri == photo.uri }) tags += "相似" to R.color.accent_blue
        if (smartInsights.blur.any { it.uri == photo.uri }) tags += "模糊" to R.color.accent_purple
        if (photo.sizeBytes >= LARGE_PHOTO_BYTES) tags += "大文件" to R.color.accent_delete
        if (tags.isEmpty()) {
            photoTagsContainer.isVisible = false
            return
        }
        photoTagsContainer.isVisible = true
        tags.forEach { (label, colorRes) ->
            photoTagsContainer.addView(createTagChip(label, getColor(colorRes)))
        }
    }

    private fun createTagChip(label: String, color: Int): TextView {
        return TextView(this).apply {
            text = label
            textSize = 9f
            setTextColor(color)
            setPadding(dp(7), dp(2), dp(7), dp(2))
            background = GradientDrawable().apply {
                cornerRadius = dp(4).toFloat()
                setColor(withAlpha(color, 0.18f))
            }
            val params = LinearLayout.LayoutParams(
                ViewGroup.LayoutParams.WRAP_CONTENT,
                ViewGroup.LayoutParams.WRAP_CONTENT
            )
            params.marginEnd = dp(6)
            layoutParams = params
        }
    }

    private fun updateWelcomeSummary() {
        statTotalPhotosText.text = formatCount(allPhotos.size)
        val releasablePhotos = (smartInsights.screenshots + smartInsights.similar + smartInsights.blur)
            .distinctBy { it.uri }
        val releasableBytes = releasablePhotos.sumOf { it.sizeBytes }
        statReleasableText.text = formatStorage(releasableBytes)
        statEstimatedTimeText.text = "~${max(1, releasablePhotos.size / 12)} min"
        deletedTotalText.text = "累计已删除 ${readDeletedTotal()} 张照片"
    }

    private fun updateSmartCardMeta() {
        screenshotMetaText.text = formatSuggestionMeta(smartInsights.screenshots)
        similarMetaText.text = formatSuggestionMeta(smartInsights.similar)
        blurMetaText.text = formatSuggestionMeta(smartInsights.blur)
    }

    private fun formatSuggestionMeta(items: List<PhotoItem>): String {
        return if (items.isEmpty()) {
            "未发现明显建议项"
        } else {
            "${items.size} 张 · 约 ${formatStorage(items.sumOf { it.sizeBytes })}"
        }
    }

    private fun updateReviewProgress() {
        val done = reviewStats.keptCount + reviewStats.markedDeleteCount + reviewStats.skippedCount
        val percent = if (currentBatchSize == 0) 0 else done * 100 / currentBatchSize
        val progressValue = if (currentBatchSize == 0) 0 else done * 1000 / currentBatchSize
        progressAnimator?.cancel()
        progressAnimator = ObjectAnimator.ofInt(progressBar, "progress", progressBar.progress, progressValue).apply {
            duration = 640L
            interpolator = AccelerateDecelerateInterpolator()
            start()
        }
        progressDoneText.text = "已处理 $done 张"
        progressPercentText.text = "$percent%"
        viewerCounterText.text = if (currentBatchSize == 0) "0 / 0" else "${min(done + 1, currentBatchSize)} / $currentBatchSize"
        undoButton.alpha = if (reviewHistory.isEmpty()) 0.72f else 1f
    }

    private fun updateResultUi() {
        resultKeptText.text = reviewStats.keptCount.toString()
        resultDeletedText.text = reviewStats.markedDeleteCount.toString()
        resultSkippedText.text = reviewStats.skippedCount.toString()
        resultFreedText.text = formatStorage(reviewStats.releasableBytes)
        resultDeleteButton.isEnabled = pendingDeleteUris.isNotEmpty()
        resultDeleteButton.alpha = if (pendingDeleteUris.isNotEmpty()) 1f else 0.5f
        resultDeleteButton.text = if (pendingDeleteUris.isEmpty()) {
            "本轮没有待删除照片"
        } else {
            "删除本轮标记的 ${pendingDeleteUris.size} 张照片"
        }
        nextBatchButton.text = if (allPhotos.isEmpty()) "重新扫描相册" else "继续整理下一批 →"
    }

    private fun updateHistoryUi() {
        historyFreedTotalText.text = formatStorage(readFreedBytesTotal())
        renderStreak()
        renderBadges()
        renderHistoryEntries()
    }

    private fun renderStreak() {
        historyStreakContainer.removeAllViews()
        val today = LocalDate.now(zoneId)
        val activeDays = historyRecords.map {
            Instant.ofEpochMilli(it.timestamp).atZone(zoneId).toLocalDate()
        }.toSet()

        for (offset in 6 downTo 0) {
            val day = today.minusDays(offset.toLong())
            val label = if (offset == 0) "今日" else DAY_LABELS[day.dayOfWeek.value - 1]
            val item = LinearLayout(this).apply {
                orientation = LinearLayout.HORIZONTAL
                gravity = Gravity.CENTER_VERTICAL
                val params = LinearLayout.LayoutParams(
                    ViewGroup.LayoutParams.WRAP_CONTENT,
                    ViewGroup.LayoutParams.WRAP_CONTENT
                )
                params.marginEnd = dp(8)
                layoutParams = params
            }
            item.addView(View(this).apply {
                layoutParams = LinearLayout.LayoutParams(dp(7), dp(7)).apply { marginEnd = dp(3) }
                background = GradientDrawable().apply {
                    shape = GradientDrawable.OVAL
                    setColor(getColor(if (activeDays.contains(day)) R.color.accent_keep else R.color.surface_4))
                }
            })
            item.addView(TextView(this).apply {
                text = label
                textSize = 10f
                setTextColor(getColor(R.color.text_secondary))
            })
            historyStreakContainer.addView(item)
        }
    }

    private fun renderBadges() {
        badgeContainer.removeAllViews()
        val badges = listOf(
            Triple("🏆 初次整理", historyRecords.isNotEmpty(), "green"),
            Triple("💾 释放 1GB", readFreedBytesTotal() >= ONE_GB, "amber"),
            Triple("🔥 连续3天", currentStreakDays() >= 3, "blue"),
            Triple("🔒 整理大师", readDeletedTotal() >= 100, "purple")
        )
        badges.forEachIndexed { index, (label, earned, accent) ->
            badgeContainer.addView(createBadge(label, earned, accent).apply {
                if (index < badges.lastIndex) {
                    (layoutParams as LinearLayout.LayoutParams).marginEnd = dp(8)
                }
            })
        }
    }

    private fun createBadge(label: String, earned: Boolean, accent: String): TextView {
        return TextView(this).apply {
            text = label
            textSize = 10f
            setPadding(dp(10), dp(8), dp(10), dp(8))
            setTextColor(
                if (earned) {
                    getColor(accentColorRes(accent))
                } else {
                    getColor(R.color.text_muted)
                }
            )
            background = GradientDrawable().apply {
                cornerRadius = dp(10).toFloat()
                setColor(if (earned) withAlpha(getColor(accentColorRes(accent)), 0.14f) else getColor(R.color.surface_3))
                setStroke(dp(1), if (earned) withAlpha(getColor(accentColorRes(accent)), 0.35f) else getColor(R.color.border))
            }
            layoutParams = LinearLayout.LayoutParams(
                ViewGroup.LayoutParams.WRAP_CONTENT,
                ViewGroup.LayoutParams.WRAP_CONTENT
            )
        }
    }

    private fun renderHistoryEntries() {
        historyEntryContainer.removeAllViews()
        if (historyRecords.isEmpty()) {
            historyEntryContainer.addView(TextView(this).apply {
                text = "还没有整理记录，完成一轮清理后会显示在这里。"
                textSize = 12f
                setTextColor(getColor(R.color.text_muted))
                setPadding(0, dp(12), 0, 0)
            })
            return
        }

        historyRecords.take(8).forEach { record ->
            val row = LinearLayout(this).apply {
                orientation = LinearLayout.HORIZONTAL
                gravity = Gravity.CENTER_VERTICAL
                setPadding(0, dp(12), 0, dp(12))
                background = GradientDrawable().apply {
                    setColor(Color.TRANSPARENT)
                    setStroke(0, Color.TRANSPARENT)
                }
            }

            val accent = getColor(accentColorRes(record.accent))
            row.addView(TextView(this).apply {
                text = record.icon
                gravity = Gravity.CENTER
                textSize = 16f
                background = GradientDrawable().apply {
                    cornerRadius = dp(10).toFloat()
                    setColor(withAlpha(accent, 0.18f))
                }
                layoutParams = LinearLayout.LayoutParams(dp(36), dp(36))
            })

            row.addView(LinearLayout(this).apply {
                orientation = LinearLayout.VERTICAL
                layoutParams = LinearLayout.LayoutParams(0, ViewGroup.LayoutParams.WRAP_CONTENT, 1f).apply {
                    marginStart = dp(12)
                    marginEnd = dp(12)
                }
                addView(TextView(this@MainActivity).apply {
                    text = entryTimeFormatter.format(Date(record.timestamp))
                    textSize = 12f
                    setTextColor(getColor(R.color.text_primary))
                })
                addView(TextView(this@MainActivity).apply {
                    text = "${record.title} · ${record.meta}"
                    textSize = 10f
                    setTextColor(getColor(R.color.text_muted))
                })
            })

            row.addView(TextView(this).apply {
                text = formatStorage(record.freedBytes)
                textSize = 10f
                setTextColor(accent)
                setPadding(dp(8), dp(4), dp(8), dp(4))
                background = GradientDrawable().apply {
                    cornerRadius = dp(6).toFloat()
                    setColor(withAlpha(accent, 0.16f))
                }
            })

            val divider = View(this).apply {
                layoutParams = LinearLayout.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, dp(1))
                setBackgroundColor(getColor(R.color.border))
            }

            historyEntryContainer.addView(row)
            historyEntryContainer.addView(divider)
        }
        if (historyEntryContainer.childCount > 0) {
            historyEntryContainer.removeViewAt(historyEntryContainer.childCount - 1)
        }
    }

    private fun requestDeletePending(uris: Collection<Uri>): PendingIntent? {
        return runCatching {
            MediaStore.createDeleteRequest(contentResolver, uris.toList())
        }.getOrNull()
    }

    private fun attachSwipeGesture() {
        var downX = 0f
        photoCard.setOnTouchListener { view, event ->
            when (event.actionMasked) {
                MotionEvent.ACTION_DOWN -> {
                    downX = event.rawX
                    resetSwipePreview(immediate = true)
                    true
                }
                MotionEvent.ACTION_MOVE -> {
                    val deltaX = event.rawX - downX
                    view.translationX = deltaX
                    view.rotation = deltaX / 45f
                    updateSwipePreview(deltaX)
                    true
                }
                MotionEvent.ACTION_UP,
                MotionEvent.ACTION_CANCEL -> {
                    val deltaX = event.rawX - downX
                    when {
                        deltaX > SWIPE_COMMIT_THRESHOLD -> handleReviewAction(ActionType.KEEP)
                        deltaX < -SWIPE_COMMIT_THRESHOLD -> handleReviewAction(ActionType.MARK_DELETE)
                        else -> {
                            resetSwipePreview(immediate = false)
                            view.animate().translationX(0f).rotation(0f).setDuration(180).start()
                        }
                    }
                    true
                }
                else -> false
            }
        }
    }

    private fun showSwipeStatus(action: ActionType) {
        swipeStatusText.isVisible = false
        when (action) {
            ActionType.KEEP -> {
                swipeKeepOverlay.isVisible = true
                swipeDeleteOverlay.isVisible = false
                swipeKeepOverlay.alpha = 1f
                swipeDeleteOverlay.alpha = 0f
            }
            ActionType.MARK_DELETE -> {
                swipeDeleteOverlay.isVisible = true
                swipeKeepOverlay.isVisible = false
                swipeDeleteOverlay.alpha = 1f
                swipeKeepOverlay.alpha = 0f
            }
            ActionType.SKIP -> resetSwipePreview(immediate = true)
        }
    }

    private fun updateSwipePreview(deltaX: Float) {
        val normalized = (abs(deltaX) / SWIPE_COMMIT_THRESHOLD).coerceIn(0f, 1f)
        when {
            deltaX > 0f -> {
                swipeKeepOverlay.isVisible = true
                swipeDeleteOverlay.isVisible = false
                swipeKeepOverlay.alpha = normalized
                swipeDeleteOverlay.alpha = 0f
            }
            deltaX < 0f -> {
                swipeDeleteOverlay.isVisible = true
                swipeKeepOverlay.isVisible = false
                swipeDeleteOverlay.alpha = normalized
                swipeKeepOverlay.alpha = 0f
            }
            else -> resetSwipePreview(immediate = true)
        }
        swipeStatusText.isVisible = false
    }

    private fun resetSwipePreview(immediate: Boolean) {
        swipeStatusText.isVisible = false
        if (immediate) {
            swipeKeepOverlay.alpha = 0f
            swipeDeleteOverlay.alpha = 0f
            swipeKeepOverlay.isVisible = false
            swipeDeleteOverlay.isVisible = false
            return
        }
        swipeKeepOverlay.animate().alpha(0f).setDuration(180).withEndAction {
            swipeKeepOverlay.isVisible = false
        }.start()
        swipeDeleteOverlay.animate().alpha(0f).setDuration(180).withEndAction {
            swipeDeleteOverlay.isVisible = false
        }.start()
    }

    private fun loadScaledBitmap(uri: Uri, maxEdge: Int): Bitmap? {
        return runCatching {
            val source = ImageDecoder.createSource(contentResolver, uri)
            ImageDecoder.decodeBitmap(source) { decoder, info, _ ->
                decoder.allocator = ImageDecoder.ALLOCATOR_SOFTWARE
                val width = info.size.width
                val height = info.size.height
                val scale = minOf(1f, maxEdge / width.toFloat(), maxEdge / height.toFloat())
                decoder.setTargetSize(
                    max(1, (width * scale).toInt()),
                    max(1, (height * scale).toInt())
                )
            }
        }.getOrNull()
    }

    private fun checkForUpdates(userInitiated: Boolean) {
        if (updateCheckInProgress) return
        updateCheckInProgress = true
        checkUpdateButton.isEnabled = false
        checkUpdateButton.text = "检查中…"
        updateWorker.execute {
            val updateInfo = fetchLatestRelease()
            runOnUiThread {
                updateCheckInProgress = false
                checkUpdateButton.isEnabled = true
                checkUpdateButton.text = "更新"
                when {
                    updateInfo == null -> {
                        if (userInitiated) {
                            showToast("暂时无法检查更新，请稍后再试")
                        }
                    }
                    isRemoteVersionNewer(updateInfo.versionLabel, readCurrentVersionName()) -> {
                        showUpdateDialog(updateInfo)
                    }
                    userInitiated -> {
                        showToast("当前已是最新版本")
                    }
                }
            }
        }
    }

    private fun fetchLatestRelease(): UpdateInfo? {
        return runCatching {
            val connection = (URL(LATEST_RELEASE_API_URL).openConnection() as HttpURLConnection).apply {
                requestMethod = "GET"
                connectTimeout = 4000
                readTimeout = 5000
                setRequestProperty("Accept", "application/vnd.github+json")
                setRequestProperty("X-GitHub-Api-Version", "2022-11-28")
            }
            connection.inputStream.use { input ->
                if (connection.responseCode !in 200..299) return null
                val payload = input.bufferedReader().use { it.readText() }
                val json = JSONObject(payload)
                val tagName = json.optString("tag_name")
                val versionLabel = normalizeVersionLabel(tagName.ifBlank { json.optString("name") })
                val htmlUrl = json.optString("html_url")
                val notes = json.optString("body")
                val releaseTitle = json.optString("name").ifBlank { "v$versionLabel" }
                val assets = json.optJSONArray("assets")
                val firstAsset = assets?.optJSONObject(0)
                val downloadUrl = firstAsset?.optString("browser_download_url")?.takeIf { it.isNotBlank() }
                val assetName = firstAsset?.optString("name")?.takeIf { it.isNotBlank() }
                if (versionLabel.isBlank() || htmlUrl.isBlank()) {
                    null
                } else {
                    UpdateInfo(
                        versionLabel = versionLabel,
                        releaseTitle = releaseTitle,
                        notes = notes,
                        htmlUrl = htmlUrl,
                        downloadUrl = downloadUrl,
                        assetName = assetName
                    )
                }
            }
        }.getOrNull()
    }

    private fun normalizeVersionLabel(raw: String): String {
        return raw.trim().removePrefix("v").removePrefix("V").trim()
    }

    private fun isRemoteVersionNewer(remoteVersion: String, localVersion: String): Boolean {
        val remoteParts = parseVersionParts(remoteVersion)
        val localParts = parseVersionParts(localVersion)
        val maxParts = max(remoteParts.size, localParts.size)
        for (index in 0 until maxParts) {
            val remotePart = remoteParts.getOrElse(index) { 0 }
            val localPart = localParts.getOrElse(index) { 0 }
            if (remotePart != localPart) {
                return remotePart > localPart
            }
        }
        return false
    }

    private fun parseVersionParts(version: String): List<Int> {
        return Regex("\\d+").findAll(version).map { it.value.toIntOrNull() ?: 0 }.toList().ifEmpty { listOf(0) }
    }

    private fun showUpdateDialog(updateInfo: UpdateInfo) {
        val message = buildString {
            append("当前版本 v${readCurrentVersionName()}\n")
            append("最新版本 v${updateInfo.versionLabel}")
            val trimmedNotes = updateInfo.notes.trim()
            if (trimmedNotes.isNotEmpty()) {
                append("\n\n更新说明\n")
                append(trimmedNotes.take(MAX_UPDATE_NOTES_LENGTH))
                if (trimmedNotes.length > MAX_UPDATE_NOTES_LENGTH) {
                    append("…")
                }
            }
        }
        AlertDialog.Builder(this)
            .setTitle(updateInfo.releaseTitle)
            .setMessage(message)
            .setNegativeButton("稍后", null)
            .setPositiveButton("立即下载") { _, _ ->
                startInAppUpdateDownload(updateInfo)
            }
            .show()
    }

    private fun startInAppUpdateDownload(updateInfo: UpdateInfo) {
        val downloadUrl = updateInfo.downloadUrl
        val assetName = updateInfo.assetName
        if (downloadUrl.isNullOrBlank() || assetName.isNullOrBlank()) {
            showToast("当前版本暂时没有可下载的安装包")
            return
        }
        val destinationDir = getExternalFilesDir(Environment.DIRECTORY_DOWNLOADS)
        if (destinationDir == null) {
            showToast("暂时无法创建下载目录")
            return
        }
        File(destinationDir, assetName).delete()
        val request = DownloadManager.Request(Uri.parse(downloadUrl)).apply {
            setTitle("下载更新 ${updateInfo.versionLabel}")
            setDescription("正在下载 ${assetName}")
            setMimeType(APK_MIME_TYPE)
            setNotificationVisibility(DownloadManager.Request.VISIBILITY_VISIBLE_NOTIFY_COMPLETED)
            setAllowedOverMetered(true)
            setAllowedOverRoaming(true)
            setDestinationInExternalFilesDir(
                this@MainActivity,
                Environment.DIRECTORY_DOWNLOADS,
                assetName
            )
        }
        updateDownloadId = downloadManager.enqueue(request)
        pendingInstallFileName = assetName
        showToast("已开始下载更新包")
    }

    private fun handleUpdateDownloadComplete(downloadId: Long) {
        val query = DownloadManager.Query().setFilterById(downloadId)
        downloadManager.query(query)?.use { cursor ->
            if (!cursor.moveToFirst()) {
                showToast("更新下载结果读取失败")
                return
            }
            val status = cursor.getInt(cursor.getColumnIndexOrThrow(DownloadManager.COLUMN_STATUS))
            updateDownloadId = -1L
            if (status == DownloadManager.STATUS_SUCCESSFUL) {
                val fileName = pendingInstallFileName
                if (fileName.isNullOrBlank()) {
                    showToast("下载完成，但未找到安装包")
                    return
                }
                showToast("下载完成，准备安装更新")
                installDownloadedApk(fileName)
            } else {
                pendingInstallFileName = null
                showToast("更新下载失败，请稍后重试")
            }
        } ?: run {
            updateDownloadId = -1L
            pendingInstallFileName = null
            showToast("更新下载失败，请稍后重试")
        }
    }

    private fun installDownloadedApk(fileName: String) {
        if (!packageManager.canRequestPackageInstalls()) {
            pendingInstallFileName = fileName
            AlertDialog.Builder(this)
                .setTitle("需要安装权限")
                .setMessage("请允许“相册清理助手”安装未知应用，授权后会继续安装更新。")
                .setNegativeButton("稍后", null)
                .setPositiveButton("去授权") { _, _ ->
                    startActivity(
                        Intent(
                            Settings.ACTION_MANAGE_UNKNOWN_APP_SOURCES,
                            Uri.parse("package:$packageName")
                        )
                    )
                }
                .show()
            return
        }
        val downloadDir = getExternalFilesDir(Environment.DIRECTORY_DOWNLOADS) ?: run {
            showToast("未找到已下载的安装包")
            return
        }
        val apkFile = File(downloadDir, fileName)
        if (!apkFile.exists()) {
            showToast("未找到已下载的安装包")
            return
        }
        val uri = FileProvider.getUriForFile(this, "$packageName.fileprovider", apkFile)
        val intent = Intent(Intent.ACTION_VIEW).apply {
            setDataAndType(uri, APK_MIME_TYPE)
            addFlags(Intent.FLAG_ACTIVITY_NEW_TASK)
            addFlags(Intent.FLAG_GRANT_READ_URI_PERMISSION)
        }
        runCatching {
            startActivity(intent)
            pendingInstallFileName = null
        }.onFailure {
            showToast("暂时无法打开安装界面")
        }
    }

    private fun readCurrentVersionName(): String {
        return runCatching {
            packageManager.getPackageInfo(packageName, 0).versionName
        }.getOrNull().orEmpty().ifBlank { "1.0" }
    }

    private fun preloadSmartThumbnails(insights: SmartInsights) {
        (insights.screenshots + insights.similar + insights.blur)
            .distinctBy { it.uri }
            .take(MAX_SMART_THUMB_PRELOAD)
            .forEach { photo ->
                val key = smartThumbKey(photo.uri)
                if (smartThumbCache.get(key) != null) return@forEach
                smartThumbWorker.execute {
                    val bitmap = loadScaledBitmap(photo.uri, 360) ?: return@execute
                    smartThumbCache.put(key, bitmap)
                }
            }
    }

    private fun smartThumbKey(uri: Uri): String = "${uri}@360"

    private fun navigateTo(screen: Screen) {
        showScreen(screen, addToBackStack = true)
    }

    private fun returnToWelcome() {
        screenBackStack.clear()
        showScreen(Screen.WELCOME, addToBackStack = false)
    }

    private fun navigateBack(): Boolean {
        if (currentScreen == Screen.RESULT) {
            returnToWelcome()
            return true
        }
        while (screenBackStack.isNotEmpty()) {
            val previous = screenBackStack.removeLast()
            if (previous != currentScreen) {
                showScreen(previous, addToBackStack = false)
                return true
            }
        }
        return false
    }

    private fun showScreen(screen: Screen, addToBackStack: Boolean) {
        if (addToBackStack && currentScreen != screen) {
            screenBackStack.addLast(currentScreen)
        }
        currentScreen = screen
        welcomeContainer.isVisible = screen == Screen.WELCOME
        smartContainer.isVisible = screen == Screen.SMART
        viewerContainer.isVisible = screen == Screen.VIEWER
        resultContainer.isVisible = screen == Screen.RESULT
        historyContainer.isVisible = screen == Screen.HISTORY
    }

    private fun attachPressFeedback(view: View) {
        view.setOnTouchListener { touchedView, event ->
            when (event.actionMasked) {
                MotionEvent.ACTION_DOWN -> {
                    touchedView.animate().scaleX(0.88f).scaleY(0.88f).setDuration(110).start()
                }
                MotionEvent.ACTION_UP,
                MotionEvent.ACTION_CANCEL -> {
                    touchedView.animate().scaleX(1f).scaleY(1f).setDuration(140).start()
                }
            }
            false
        }
    }

    private fun startShimmerAnimation() {
        startButtonShimmer.post {
            val width = startButton.width.takeIf { it > 0 } ?: return@post
            val travel = width * 1.2f
            startButtonShimmer.translationX = -travel
            ObjectAnimator.ofFloat(
                startButtonShimmer,
                View.TRANSLATION_X,
                -travel,
                travel
            ).apply {
                duration = 2800L
                repeatCount = ObjectAnimator.INFINITE
                repeatMode = ObjectAnimator.RESTART
                interpolator = AccelerateDecelerateInterpolator()
                startDelay = 1200L
                start()
            }
        }
    }

    private fun hasReadPermission(): Boolean {
        return ContextCompat.checkSelfPermission(
            this,
            Manifest.permission.READ_MEDIA_IMAGES
        ) == PackageManager.PERMISSION_GRANTED
    }

    private fun buildMetaText(photo: PhotoItem): String {
        val dimension = if (photo.width > 0 && photo.height > 0) {
            "${photo.width}×${photo.height}"
        } else {
            "尺寸未知"
        }
        return "${formatStorageExact(photo.sizeBytes)} · $dimension"
    }

    private fun formatStorage(bytes: Long): String {
        if (bytes <= 0L) return "0 MB"
        val gb = bytes / 1024f / 1024f / 1024f
        return if (gb >= 1f) "${decimalFormat.format(gb)} GB" else "${decimalFormat.format(bytes / 1024f / 1024f)} MB"
    }

    private fun formatStorageExact(bytes: Long): String {
        return when {
            bytes >= ONE_GB -> "${decimalFormat.format(bytes / 1024f / 1024f / 1024f)} GB"
            bytes >= 1024L * 1024L -> "${decimalFormat.format(bytes / 1024f / 1024f)} MB"
            bytes >= 1024L -> "${decimalFormat.format(bytes / 1024f)} KB"
            else -> "$bytes B"
        }
    }

    private fun formatCount(count: Int): String {
        return "%,d".format(Locale.US, count)
    }

    private fun aspectRatioDifference(first: PhotoItem, second: PhotoItem): Float {
        if (first.width <= 0 || first.height <= 0 || second.width <= 0 || second.height <= 0) return 1f
        val firstRatio = first.width.toFloat() / first.height.toFloat()
        val secondRatio = second.width.toFloat() / second.height.toFloat()
        return abs(firstRatio - secondRatio)
    }

    private fun isScreenshotPhoto(photo: PhotoItem): Boolean {
        val name = photo.name.lowercase(Locale.getDefault())
        val path = photo.relativePath.lowercase(Locale.getDefault())
        val bucket = photo.bucketName.lowercase(Locale.getDefault())
        return name.contains("screenshot") ||
            name.contains("screen_shot") ||
            name.contains("截屏") ||
            name.contains("截图") ||
            path.contains("screenshot") ||
            path.contains("screenshots") ||
            bucket.contains("screenshot") ||
            bucket.contains("截屏") ||
            bucket.contains("截图")
    }

    private fun currentStreakDays(): Int {
        val activeDays = historyRecords.map {
            Instant.ofEpochMilli(it.timestamp).atZone(zoneId).toLocalDate()
        }.toSet()
        var streak = 0
        var cursor = LocalDate.now(zoneId)
        while (activeDays.contains(cursor)) {
            streak++
            cursor = cursor.minusDays(1)
        }
        return streak
    }

    private fun accentColorRes(accent: String): Int {
        return when (accent) {
            "amber" -> R.color.accent_warm
            "blue" -> R.color.accent_blue
            "purple" -> R.color.accent_purple
            else -> R.color.accent_keep
        }
    }

    private fun withAlpha(color: Int, alpha: Float): Int {
        val a = (255 * alpha).toInt().coerceIn(0, 255)
        return Color.argb(a, Color.red(color), Color.green(color), Color.blue(color))
    }

    private fun showToast(message: String) {
        Toast.makeText(this, message, Toast.LENGTH_SHORT).show()
    }

    private fun applySavedThemeMode() {
        val mode = getSharedPreferences(PREFS_NAME, MODE_PRIVATE)
            .getInt(KEY_THEME_MODE, AppCompatDelegate.MODE_NIGHT_YES)
        AppCompatDelegate.setDefaultNightMode(mode)
    }

    private fun toggleThemeMode() {
        val nextMode = if (AppCompatDelegate.getDefaultNightMode() == AppCompatDelegate.MODE_NIGHT_YES) {
            AppCompatDelegate.MODE_NIGHT_NO
        } else {
            AppCompatDelegate.MODE_NIGHT_YES
        }
        preferences.edit().putInt(KEY_THEME_MODE, nextMode).apply()
        AppCompatDelegate.setDefaultNightMode(nextMode)
        updateThemeToggleText()
    }

    private fun updateThemeToggleText() {
        val isDark = resources.configuration.uiMode and android.content.res.Configuration.UI_MODE_NIGHT_MASK ==
            android.content.res.Configuration.UI_MODE_NIGHT_YES
        themeToggleButton.text = if (isDark) "☀ 日间" else "🌙 夜间"
    }

    private fun dp(value: Int): Int {
        return (value * resources.displayMetrics.density).toInt()
    }

    private fun readDeletedTotal(): Int {
        return preferences.getInt(KEY_DELETED_TOTAL, 0)
    }

    private fun saveDeletedTotal(value: Int) {
        preferences.edit().putInt(KEY_DELETED_TOTAL, value).apply()
    }

    private fun readFreedBytesTotal(): Long {
        return preferences.getLong(KEY_FREED_TOTAL, 0L)
    }

    private fun saveFreedBytesTotal(value: Long) {
        preferences.edit().putLong(KEY_FREED_TOTAL, value).apply()
    }

    private fun loadHistory() {
        historyRecords.clear()
        val raw = preferences.getString(KEY_HISTORY_JSON, null) ?: return
        val array = runCatching { JSONArray(raw) }.getOrNull() ?: return
        for (index in 0 until array.length()) {
            val item = array.optJSONObject(index) ?: continue
            historyRecords.add(
                HistoryRecord(
                    timestamp = item.optLong("timestamp"),
                    title = item.optString("title"),
                    meta = item.optString("meta"),
                    freedBytes = item.optLong("freedBytes"),
                    icon = item.optString("icon"),
                    accent = item.optString("accent")
                )
            )
        }
    }

    private fun saveHistory() {
        val array = JSONArray()
        historyRecords.take(MAX_HISTORY_RECORDS).forEach { record ->
            array.put(
                JSONObject().apply {
                    put("timestamp", record.timestamp)
                    put("title", record.title)
                    put("meta", record.meta)
                    put("freedBytes", record.freedBytes)
                    put("icon", record.icon)
                    put("accent", record.accent)
                }
            )
        }
        preferences.edit().putString(KEY_HISTORY_JSON, array.toString()).apply()
        if (historyRecords.size > MAX_HISTORY_RECORDS) {
            historyRecords.subList(MAX_HISTORY_RECORDS, historyRecords.size).clear()
        }
    }

    private enum class Screen {
        WELCOME,
        SMART,
        VIEWER,
        RESULT,
        HISTORY
    }

    companion object {
        private const val APK_MIME_TYPE = "application/vnd.android.package-archive"
        private const val LATEST_RELEASE_API_URL = "https://api.github.com/repos/DZHAPPY/PhotoCleaner/releases/latest"
        private const val MAX_REVIEW_BATCH = 20
        private const val MAX_SMART_ITEMS = 60
        private const val MAX_SMART_THUMB_PRELOAD = 90
        private const val MAX_UPDATE_NOTES_LENGTH = 280
        private const val MAX_BLUR_ANALYSIS = 80
        private const val SWIPE_PREVIEW_THRESHOLD = 64f
        private const val SWIPE_COMMIT_THRESHOLD = 180f
        private const val BLUR_THRESHOLD = 1800.0
        private const val SIMILAR_WINDOW_MILLIS = 45_000L
        private const val LARGE_PHOTO_BYTES = 5L * 1024L * 1024L
        private const val ONE_GB = 1024L * 1024L * 1024L
        private const val MAX_HISTORY_RECORDS = 20
        private const val PREFS_NAME = "photo_cleaner_prefs"
        private const val KEY_DELETED_TOTAL = "deleted_total"
        private const val KEY_FREED_TOTAL = "freed_total"
        private const val KEY_HISTORY_JSON = "history_json"
        private const val KEY_THEME_MODE = "theme_mode"
        private val DAY_LABELS = listOf("周一", "周二", "周三", "周四", "周五", "周六", "周日")
    }
}
