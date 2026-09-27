package org.schabi.newpipe.util.universal;

import android.net.Uri;
import android.text.TextUtils;
import android.util.Log;

import androidx.annotation.NonNull;
import androidx.annotation.Nullable;

import org.json.JSONArray;
import org.json.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.schabi.newpipe.DownloaderImpl;
import org.schabi.newpipe.extractor.MediaFormat;
import org.schabi.newpipe.extractor.ServiceList;
import org.schabi.newpipe.extractor.exceptions.ExtractionException;
import org.schabi.newpipe.extractor.stream.AudioStream;
import org.schabi.newpipe.extractor.stream.DeliveryMethod;
import org.schabi.newpipe.extractor.stream.StreamInfo;
import org.schabi.newpipe.extractor.stream.StreamType;
import org.schabi.newpipe.extractor.stream.SubtitlesStream;
import org.schabi.newpipe.extractor.stream.VideoStream;

import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.reactivex.rxjava3.core.Single;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

/**
 * Universal media extractor capable of resolving video and audio streams from
 * direct media links, social media platforms (TikTok, Instagram, Twitter/X, Reddit, Facebook),
 * and generic HTML5 websites.
 */
public final class UniversalMediaExtractor {
    private static final String TAG = "UniversalExtractor";

    private static final String USER_AGENT =
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    + "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36";

    // Direct media file extensions
    private static final Pattern DIRECT_VIDEO_PATTERN =
            Pattern.compile("\\.(mp4|webm|mkv|mov|flv|m4v)(\\?.*)?$", Pattern.CASE_INSENSITIVE);
    private static final Pattern DIRECT_AUDIO_PATTERN =
            Pattern.compile("\\.(mp3|m4a|aac|ogg|opus|wav|flac)(\\?.*)?$",
                    Pattern.CASE_INSENSITIVE);
    private static final Pattern DIRECT_HLS_PATTERN =
            Pattern.compile("\\.m3u8(\\?.*)?$", Pattern.CASE_INSENSITIVE);

    private UniversalMediaExtractor() { }

    /**
     * Resolve stream information for any supported or arbitrary URL.
     *
     * @param url the source URL from any website or social media platform
     * @return Single emitting the resolved StreamInfo
     */
    @NonNull
    public static Single<StreamInfo> extract(@NonNull final String url) {
        return Single.fromCallable(() -> extractSynchronous(url.trim()));
    }

    @NonNull
    private static StreamInfo extractSynchronous(@NonNull final String url)
            throws ExtractionException {
        final OkHttpClient client = DownloaderImpl.getInstance() != null
                ? DownloaderImpl.getInstance().getClient()
                : new OkHttpClient();

        // 1. Check for direct media URL by extension first
        if (DIRECT_VIDEO_PATTERN.matcher(url).find()
                || DIRECT_AUDIO_PATTERN.matcher(url).find()
                || DIRECT_HLS_PATTERN.matcher(url).find()) {
            final StreamInfo directInfo = buildDirectMediaStreamInfo(url, null);
            if (directInfo != null) {
                return directInfo;
            }
        }

        // 2. Perform HTTP request to examine headers and page content
        final Request request = new Request.Builder()
                .url(url)
                .header("User-Agent", USER_AGENT)
                .header("Accept", "*/*")
                .build();

        final String responseBody;
        final String contentType;
        final String effectiveUrl;

        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw new ExtractionException("HTTP error " + response.code() + " for " + url);
            }

            effectiveUrl = response.request().url().toString();
            contentType = response.header("Content-Type", "");

            // If the server returns a media content type directly
            if (contentType.startsWith("video/") || contentType.startsWith("audio/")
                    || contentType.contains("mpegurl")) {
                final String lengthHeader = response.header("Content-Length");
                long contentLength = -1;
                if (!TextUtils.isEmpty(lengthHeader)) {
                    try {
                        contentLength = Long.parseLong(lengthHeader);
                    } catch (final NumberFormatException ignored) { }
                }
                final StreamInfo directMedia = buildDirectMediaStreamInfo(effectiveUrl,
                        contentType);
                if (directMedia != null) {
                    return directMedia;
                }
            }

            if (response.body() != null) {
                responseBody = response.body().string();
            } else {
                responseBody = "";
            }
        } catch (final IOException e) {
            throw new ExtractionException("Failed to fetch content from " + url, e);
        }

        // 3. Platform-specific handlers
        final Uri uri = Uri.parse(effectiveUrl);
        final String host = uri.getHost() != null ? uri.getHost().toLowerCase(Locale.ROOT) : "";

        if (host.contains("reddit.com") || host.contains("v.redd.it")) {
            final StreamInfo redditInfo = extractReddit(client, effectiveUrl, responseBody);
            if (redditInfo != null) {
                return redditInfo;
            }
        }

        if (host.contains("tiktok.com")) {
            final StreamInfo tiktokInfo = extractTikTok(effectiveUrl, responseBody);
            if (tiktokInfo != null) {
                return tiktokInfo;
            }
        }

        // 4. Generic HTML5 / OpenGraph / Schema.org extraction
        final StreamInfo genericInfo = extractGenericHtml(effectiveUrl, responseBody);
        if (genericInfo != null) {
            return genericInfo;
        }

        throw new ExtractionException("No playable or downloadable media found on page: " + url);
    }

    @Nullable
    private static StreamInfo buildDirectMediaStreamInfo(@NonNull final String mediaUrl,
                                                         @Nullable final String contentType) {
        final String cleanUrl = mediaUrl.split("\\?")[0];
        final String fileName = cleanUrl.substring(cleanUrl.lastIndexOf('/') + 1);
        final String title = !TextUtils.isEmpty(fileName) ? fileName : "Universal Media";

        final boolean isAudio = (contentType != null && contentType.startsWith("audio/"))
                || DIRECT_AUDIO_PATTERN.matcher(mediaUrl).find();
        final boolean isHls = (contentType != null && contentType.contains("mpegurl"))
                || DIRECT_HLS_PATTERN.matcher(mediaUrl).find();

        final StreamInfo streamInfo = new StreamInfo(
                ServiceList.MediaCCC.getServiceId(),
                mediaUrl,
                mediaUrl,
                isAudio ? StreamType.AUDIO_STREAM : StreamType.VIDEO_STREAM,
                String.valueOf(mediaUrl.hashCode()),
                title,
                0
        );

        if (isAudio) {
            final MediaFormat format = resolveAudioFormat(mediaUrl, contentType);
            final AudioStream audioStream = new AudioStream.Builder()
                    .setId("audio_0")
                    .setContent(mediaUrl, true)
                    .setMediaFormat(format)
                    .setAverageBitrate(128)
                    .setDeliveryMethod(isHls ? DeliveryMethod.HLS : DeliveryMethod.PROGRESSIVE_HTTP)
                    .build();
            streamInfo.setAudioStreams(Collections.singletonList(audioStream));
            streamInfo.setVideoStreams(Collections.emptyList());
            streamInfo.setVideoOnlyStreams(Collections.emptyList());
        } else {
            final MediaFormat format = resolveVideoFormat(mediaUrl, contentType);
            final VideoStream videoStream = new VideoStream.Builder()
                    .setId("video_0")
                    .setContent(mediaUrl, true)
                    .setMediaFormat(format)
                    .setResolution("Original")
                    .setIsVideoOnly(false)
                    .setDeliveryMethod(isHls ? DeliveryMethod.HLS : DeliveryMethod.PROGRESSIVE_HTTP)
                    .build();
            streamInfo.setVideoStreams(Collections.singletonList(videoStream));
            streamInfo.setAudioStreams(Collections.emptyList());
            streamInfo.setVideoOnlyStreams(Collections.emptyList());
        }

        streamInfo.setSubtitles(Collections.emptyList());
        return streamInfo;
    }

    @Nullable
    private static StreamInfo extractReddit(@NonNull final OkHttpClient client,
                                            @NonNull final String originalUrl,
                                            @NonNull final String html) {
        String cleanUrl = originalUrl.split("\\?")[0];
        if (!cleanUrl.endsWith(".json")) {
            cleanUrl = cleanUrl + ".json";
        }

        try {
            final Request req = new Request.Builder()
                    .url(cleanUrl)
                    .header("User-Agent", USER_AGENT)
                    .build();
            try (Response resp = client.newCall(req).execute()) {
                if (resp.isSuccessful() && resp.body() != null) {
                    final String jsonStr = resp.body().string();
                    final JSONArray arr = new JSONArray(jsonStr);
                    if (arr.length() > 0) {
                        final JSONObject postObj = arr.getJSONObject(0)
                                .getJSONObject("data")
                                .getJSONArray("children")
                                .getJSONObject(0)
                                .getJSONObject("data");

                        final String title = postObj.optString("title", "Reddit Video");
                        final String fallbackUrl = postObj.optJSONObject("secure_media") != null
                                && postObj.getJSONObject("secure_media")
                                .optJSONObject("reddit_video") != null
                                ? postObj.getJSONObject("secure_media")
                                .getJSONObject("reddit_video").optString("fallback_url")
                                : null;

                        if (!TextUtils.isEmpty(fallbackUrl)) {
                            return buildRedditStreamInfo(originalUrl, title, fallbackUrl);
                        }
                    }
                }
            }
        } catch (final Exception e) {
            Log.w(TAG, "Reddit JSON extraction failed, falling back to HTML parser", e);
        }

        return extractGenericHtml(originalUrl, html);
    }

    @NonNull
    private static StreamInfo buildRedditStreamInfo(@NonNull final String originalUrl,
                                                    @NonNull final String title,
                                                    @NonNull final String fallbackUrl) {
        final StreamInfo streamInfo = new StreamInfo(
                ServiceList.MediaCCC.getServiceId(),
                originalUrl,
                originalUrl,
                StreamType.VIDEO_STREAM,
                String.valueOf(originalUrl.hashCode()),
                title,
                0
        );

        final List<VideoStream> videoOnlyStreams = new ArrayList<>();
        final String baseUrl = fallbackUrl.substring(0, fallbackUrl.lastIndexOf('/'));

        // Reddit delivers standard DASH renditions
        final String[] resolutions = {"1080", "720", "480", "360", "240"};
        for (final String res : resolutions) {
            final String streamUrl = baseUrl + "/DASH_" + res + ".mp4";
            videoOnlyStreams.add(new VideoStream.Builder()
                    .setId("reddit_v_" + res)
                    .setContent(streamUrl, true)
                    .setMediaFormat(MediaFormat.MPEG_4)
                    .setResolution(res + "p")
                    .setIsVideoOnly(true)
                    .setDeliveryMethod(DeliveryMethod.PROGRESSIVE_HTTP)
                    .build());
        }

        // Reddit separate audio stream
        final String audioUrl = baseUrl + "/DASH_AUDIO_128.mp4";
        final AudioStream audioStream = new AudioStream.Builder()
                .setId("reddit_a_128")
                .setContent(audioUrl, true)
                .setMediaFormat(MediaFormat.M4A)
                .setAverageBitrate(128)
                .setDeliveryMethod(DeliveryMethod.PROGRESSIVE_HTTP)
                .build();

        streamInfo.setVideoStreams(Collections.emptyList());
        streamInfo.setVideoOnlyStreams(videoOnlyStreams);
        streamInfo.setAudioStreams(Collections.singletonList(audioStream));
        streamInfo.setSubtitles(Collections.emptyList());
        return streamInfo;
    }

    @Nullable
    private static StreamInfo extractTikTok(@NonNull final String originalUrl,
                                            @NonNull final String html) {
        try {
            final Document doc = Jsoup.parse(html, originalUrl);
            final Element universalData = doc.getElementById("__UNIVERSAL_DATA_FOR_REHYDRATION__");
            if (universalData != null) {
                final String jsonText = universalData.data();
                final JSONObject json = new JSONObject(jsonText);
                final JSONObject defaultScope = json.optJSONObject("__DEFAULT_SCOPE__");
                if (defaultScope != null) {
                    final JSONObject detail = defaultScope.optJSONObject(
                            "webapp.video-detail");
                    if (detail != null && detail.optJSONObject("itemInfo") != null) {
                        final JSONObject itemStruct = detail.getJSONObject("itemInfo")
                                .getJSONObject("itemStruct");
                        final String desc = itemStruct.optString("desc", "TikTok Video");
                        final JSONObject videoObj = itemStruct.getJSONObject("video");
                        final String playAddr = videoObj.optString("playAddr");
                        final String downloadAddr = videoObj.optString("downloadAddr");
                        final String chosenUrl = !TextUtils.isEmpty(playAddr)
                                ? playAddr : downloadAddr;

                        if (!TextUtils.isEmpty(chosenUrl)) {
                            return createSingleStreamInfo(originalUrl, desc, chosenUrl,
                                    MediaFormat.MPEG_4);
                        }
                    }
                }
            }
        } catch (final Exception e) {
            Log.w(TAG, "TikTok script extraction failed", e);
        }

        return extractGenericHtml(originalUrl, html);
    }

    @Nullable
    private static StreamInfo extractGenericHtml(@NonNull final String originalUrl,
                                                 @NonNull final String html) {
        final Document doc = Jsoup.parse(html, originalUrl);

        // 1. Resolve Title
        String title = "";
        final Element ogTitle = doc.selectFirst("meta[property=og:title], meta[name=twitter:title]");
        if (ogTitle != null && !TextUtils.isEmpty(ogTitle.attr("content"))) {
            title = ogTitle.attr("content");
        } else if (!TextUtils.isEmpty(doc.title())) {
            title = doc.title();
        } else {
            title = "Video_" + System.currentTimeMillis();
        }

        final Set<String> candidates = new HashSet<>();

        // 2. OpenGraph / Twitter video tags
        final Elements ogVideos = doc.select(
                "meta[property=og:video], "
                        + "meta[property=og:video:url], "
                        + "meta[property=og:video:secure_url], "
                        + "meta[name=twitter:player:stream]");
        for (final Element el : ogVideos) {
            final String videoUrl = el.attr("content");
            if (isValidMediaUrl(videoUrl)) {
                candidates.add(videoUrl);
            }
        }

        // 3. HTML5 <video> elements
        final Elements videos = doc.select("video[src], video > source[src]");
        for (final Element v : videos) {
            final String src = v.attr("abs:src");
            if (isValidMediaUrl(src)) {
                candidates.add(src);
            }
        }

        // 4. Schema.org VideoObject JSON-LD
        final Elements jsonLdScripts = doc.select("script[type=application/ld+json]");
        for (final Element script : jsonLdScripts) {
            try {
                final String text = script.data();
                if (text.contains("contentUrl") || text.contains("embedUrl")) {
                    final Pattern contentUrlPattern =
                            Pattern.compile("\"contentUrl\"\\s*:\\s*\"([^\"]+)\"");
                    final Matcher matcher = contentUrlPattern.matcher(text);
                    while (matcher.find()) {
                        final String foundUrl = matcher.group(1).replace("\\/", "/");
                        if (isValidMediaUrl(foundUrl)) {
                            candidates.add(foundUrl);
                        }
                    }
                }
            } catch (final Exception ignored) { }
        }

        if (candidates.isEmpty()) {
            return null;
        }

        final List<VideoStream> videoStreams = new ArrayList<>();
        int index = 0;
        for (final String streamUrl : candidates) {
            final boolean isHls = streamUrl.contains(".m3u8");
            final VideoStream stream = new VideoStream.Builder()
                    .setId("video_" + (index++))
                    .setContent(streamUrl, true)
                    .setMediaFormat(isHls ? MediaFormat.MPEG_4 : resolveVideoFormat(streamUrl, null))
                    .setResolution("Standard")
                    .setIsVideoOnly(false)
                    .setDeliveryMethod(isHls ? DeliveryMethod.HLS : DeliveryMethod.PROGRESSIVE_HTTP)
                    .build();
            videoStreams.add(stream);
        }

        final StreamInfo streamInfo = new StreamInfo(
                ServiceList.MediaCCC.getServiceId(),
                originalUrl,
                originalUrl,
                StreamType.VIDEO_STREAM,
                String.valueOf(originalUrl.hashCode()),
                title,
                0
        );

        streamInfo.setVideoStreams(videoStreams);
        streamInfo.setVideoOnlyStreams(Collections.emptyList());
        streamInfo.setAudioStreams(Collections.emptyList());
        streamInfo.setSubtitles(Collections.emptyList());
        return streamInfo;
    }

    private static boolean isValidMediaUrl(@Nullable final String url) {
        if (TextUtils.isEmpty(url)) {
            return false;
        }
        final String lower = url.toLowerCase(Locale.ROOT);
        return lower.startsWith("http://") || lower.startsWith("https://");
    }

    @NonNull
    private static StreamInfo createSingleStreamInfo(@NonNull final String originalUrl,
                                                     @NonNull final String title,
                                                     @NonNull final String videoUrl,
                                                     @NonNull final MediaFormat format) {
        final StreamInfo streamInfo = new StreamInfo(
                ServiceList.MediaCCC.getServiceId(),
                originalUrl,
                originalUrl,
                StreamType.VIDEO_STREAM,
                String.valueOf(originalUrl.hashCode()),
                title,
                0
        );

        final VideoStream videoStream = new VideoStream.Builder()
                .setId("video_0")
                .setContent(videoUrl, true)
                .setMediaFormat(format)
                .setResolution("Standard")
                .setIsVideoOnly(false)
                .setDeliveryMethod(DeliveryMethod.PROGRESSIVE_HTTP)
                .build();

        streamInfo.setVideoStreams(Collections.singletonList(videoStream));
        streamInfo.setVideoOnlyStreams(Collections.emptyList());
        streamInfo.setAudioStreams(Collections.emptyList());
        streamInfo.setSubtitles(Collections.emptyList());
        return streamInfo;
    }

    @NonNull
    private static MediaFormat resolveVideoFormat(@NonNull final String url,
                                                  @Nullable final String contentType) {
        if (contentType != null) {
            if (contentType.contains("webm")) {
                return MediaFormat.WEBM;
            }
            if (contentType.contains("3gpp")) {
                return MediaFormat.v3GPP;
            }
        }
        if (url.contains(".webm")) {
            return MediaFormat.WEBM;
        }
        return MediaFormat.MPEG_4;
    }

    @NonNull
    private static MediaFormat resolveAudioFormat(@NonNull final String url,
                                                  @Nullable final String contentType) {
        if (contentType != null) {
            if (contentType.contains("mpeg") || contentType.contains("mp3")) {
                return MediaFormat.MP3;
            }
            if (contentType.contains("ogg") || contentType.contains("opus")) {
                return MediaFormat.WEBMA_OPUS;
            }
            if (contentType.contains("m4a") || contentType.contains("mp4")) {
                return MediaFormat.M4A;
            }
        }
        if (url.contains(".mp3")) {
            return MediaFormat.MP3;
        }
        if (url.contains(".ogg") || url.contains(".opus")) {
            return MediaFormat.WEBMA_OPUS;
        }
        return MediaFormat.M4A;
    }
}
