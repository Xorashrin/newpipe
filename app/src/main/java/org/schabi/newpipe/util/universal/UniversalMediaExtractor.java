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
import org.schabi.newpipe.extractor.NewPipe;
import org.schabi.newpipe.extractor.ServiceList;
import org.schabi.newpipe.extractor.StreamingService;
import org.schabi.newpipe.extractor.StreamingService.LinkType;
import org.schabi.newpipe.extractor.exceptions.ExtractionException;
import org.schabi.newpipe.extractor.stream.AudioStream;
import org.schabi.newpipe.extractor.stream.DeliveryMethod;
import org.schabi.newpipe.extractor.stream.StreamExtractor;
import org.schabi.newpipe.extractor.stream.StreamInfo;
import org.schabi.newpipe.extractor.stream.StreamType;
import org.schabi.newpipe.extractor.stream.VideoStream;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.reactivex.rxjava3.core.Single;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

/**
 * Universal media extractor capable of resolving video and audio streams from
 * direct media links, social media platforms (Facebook, Instagram, TikTok, Twitter/X, Reddit),
 * and generic HTML5 websites.
 */
public final class UniversalMediaExtractor {
    private static final String TAG = "UniversalExtractor";

    private static final Map<String, Long> STREAM_SIZE_CACHE = new ConcurrentHashMap<>();

    public static long getCachedSize(@Nullable final String url) {
        if (TextUtils.isEmpty(url)) {
            return -1;
        }
        final Long size = STREAM_SIZE_CACHE.get(url);
        return size != null ? size : -1;
    }

    public static void cacheSize(@Nullable final String url, final long size) {
        if (!TextUtils.isEmpty(url) && size > 0) {
            STREAM_SIZE_CACHE.put(url, size);
        }
    }

    private static final String BROWSER_USER_AGENT =
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    + "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36";

    private static final String BOT_USER_AGENT =
            "facebookexternalhit/1.1 (+http://www.facebook.com/externalhit_uatext.php)";

    private static final String MOBILE_USER_AGENT =
            "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 "
                    + "(KHTML, like Gecko) Chrome/128.0.0.0 Mobile Safari/537.36";

    // Direct media file extensions
    private static final Pattern DIRECT_VIDEO_PATTERN =
            Pattern.compile("\\.(mp4|webm|mkv|mov|flv|m4v)(\\?.*)?$", Pattern.CASE_INSENSITIVE);
    private static final Pattern DIRECT_AUDIO_PATTERN =
            Pattern.compile("\\.(mp3|m4a|aac|ogg|opus|wav|flac)(\\?.*)?$",
                    Pattern.CASE_INSENSITIVE);
    private static final Pattern DIRECT_HLS_PATTERN =
            Pattern.compile("\\.m3u8(\\?.*)?$", Pattern.CASE_INSENSITIVE);
    private static final Pattern UNICODE_ESCAPE_PATTERN =
            Pattern.compile("\\\\u([0-9a-fA-F]{4})");

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
    public static StreamInfo extractSynchronous(@NonNull final String url)
            throws ExtractionException {
        final OkHttpClient client = DownloaderImpl.getInstance() != null
                ? DownloaderImpl.getInstance().getClient()
                : new OkHttpClient();

        // 0. Check if URL belongs to any native NewPipe service (YouTube, SoundCloud, PeerTube, Bandcamp, etc.)
        try {
            final StreamingService nativeService = NewPipe.getServiceByUrl(url);
            if (nativeService != null && nativeService.getServiceId() != ServiceList.MediaCCC.getServiceId()) {
                final LinkType linkType = nativeService.getLinkTypeByUrl(url);
                if (linkType == LinkType.STREAM) {
                    return StreamInfo.getInfo(nativeService, url);
                }
            }
        } catch (final Throwable ignored) { }

        // 1. Check for direct media URL by extension first
        if (DIRECT_VIDEO_PATTERN.matcher(url).find()
                || DIRECT_AUDIO_PATTERN.matcher(url).find()
                || DIRECT_HLS_PATTERN.matcher(url).find()) {
            final StreamInfo directInfo = buildDirectMediaStreamInfo(url, null);
            if (directInfo != null) {
                return directInfo;
            }
        }

        final Uri uri = Uri.parse(url);
        final String host = uri.getHost() != null ? uri.getHost().toLowerCase(Locale.ROOT) : "";

        // 2. Platform-specific handlers FIRST (avoids failing on unauthenticated desktop GET)
        if (host.contains("facebook.com") || host.contains("fb.watch") || host.contains("fb.me")) {
            final StreamInfo fbInfo = extractFacebook(client, url);
            if (fbInfo != null) {
                return fbInfo;
            }
        }

        if (host.contains("instagram.com") || host.contains("instagr.am")) {
            final StreamInfo igInfo = extractInstagram(client, url);
            if (igInfo != null) {
                return igInfo;
            }
        }

        if (host.contains("tiktok.com")) {
            final StreamInfo tiktokInfo = extractTikTok(client, url);
            if (tiktokInfo != null) {
                return tiktokInfo;
            }
        }

        if (host.contains("twitter.com") || host.contains("x.com")) {
            final StreamInfo twitterInfo = extractTwitter(client, url);
            if (twitterInfo != null) {
                return twitterInfo;
            }
        }

        if (host.contains("reddit.com") || host.contains("v.redd.it")) {
            final StreamInfo redditInfo = extractReddit(client, url);
            if (redditInfo != null) {
                return redditInfo;
            }
        }

        // 3. Generic website handling: Perform HTTP request to examine headers and page content
        try {
            final Request request = new Request.Builder()
                    .url(url)
                    .header("User-Agent", BROWSER_USER_AGENT)
                    .header("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
                    .build();

            try (Response response = client.newCall(request).execute()) {
                final String effectiveUrl = response.request().url().toString();
                final String contentType = response.header("Content-Type", "");

                // If the server returns a media content type directly
                if (contentType.startsWith("video/") || contentType.startsWith("audio/")
                        || contentType.contains("mpegurl")) {
                    final StreamInfo directMedia = buildDirectMediaStreamInfo(effectiveUrl, contentType);
                    if (directMedia != null) {
                        return directMedia;
                    }
                }

                if (response.body() != null) {
                    final String responseBody = response.body().string();
                    final StreamInfo genericInfo = extractGenericHtml(client, effectiveUrl, responseBody);
                    if (genericInfo != null) {
                        return genericInfo;
                    }
                }
            }
        } catch (final Exception e) {
            Log.w(TAG, "Standard HTML fetch failed for " + url + ", trying bot headers", e);
        }

        // 4. Fallback with Bot User Agent for generic sites
        try {
            final Request botRequest = new Request.Builder()
                    .url(url)
                    .header("User-Agent", BOT_USER_AGENT)
                    .header("Accept", "*/*")
                    .build();

            try (Response botResponse = client.newCall(botRequest).execute()) {
                if (botResponse.body() != null) {
                    final String botBody = botResponse.body().string();
                    final StreamInfo botInfo = extractGenericHtml(client, url, botBody);
                    if (botInfo != null) {
                        return botInfo;
                    }
                }
            }
        } catch (final Exception ignored) { }

        throw new ExtractionException("No playable or downloadable media found on page: " + url);
    }

    /*//////////////////////////////////////////////////////////////////////////
    // Platform Extractors
    //////////////////////////////////////////////////////////////////////////*/

    /**
     * Extracts videos from Facebook Reels, Watch, and video posts.
     */
    @Nullable
    private static StreamInfo extractFacebook(@NonNull final OkHttpClient client,
                                              @NonNull final String originalUrl) {
        String targetUrl = originalUrl;
        // Unshorten URL if needed (e.g. fb.watch or share links)
        if (targetUrl.contains("fb.watch") || targetUrl.contains("/share/") || targetUrl.contains("fb.me")) {
            try {
                final Request headReq = new Request.Builder()
                        .url(targetUrl)
                        .header("User-Agent", MOBILE_USER_AGENT)
                        .build();
                try (Response headResp = client.newCall(headReq).execute()) {
                    targetUrl = headResp.request().url().toString();
                    if (targetUrl.contains("fb.watch") || targetUrl.contains("/share/")) {
                        final String loc = headResp.header("Location");
                        if (!TextUtils.isEmpty(loc) && isValidMediaUrl(loc)) {
                            targetUrl = loc;
                        }
                    }
                }
            } catch (final Exception e) {
                Log.w(TAG, "Failed to resolve Facebook redirect for " + originalUrl, e);
            }
        }

        // Method A: Facebook Video Plugin Embed API (works reliably without login)
        try {
            String embedTarget = targetUrl;
            if (embedTarget.contains("m.facebook.com")) {
                embedTarget = embedTarget.replace("m.facebook.com", "www.facebook.com");
            }
            final String encoded = URLEncoder.encode(embedTarget, StandardCharsets.UTF_8.name());
            final String embedUrl = "https://www.facebook.com/plugins/video.php?href=" + encoded;
            final Request embedReq = new Request.Builder()
                    .url(embedUrl)
                    .header("User-Agent", BROWSER_USER_AGENT)
                    .header("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
                    .header("Sec-Fetch-Dest", "iframe")
                    .header("Sec-Fetch-Mode", "navigate")
                    .build();

            try (Response embedResp = client.newCall(embedReq).execute()) {
                if (embedResp.body() != null) {
                    final String html = embedResp.body().string();
                    final StreamInfo info = parseFacebookEmbedHtml(client, originalUrl, html);
                    if (info != null) {
                        return info;
                    }
                }
            }
        } catch (final Exception e) {
            Log.w(TAG, "Facebook Video Plugin Embed extraction failed", e);
        }

        // Method B: Crawler Request (facebookexternalhit) to retrieve OpenGraph / native URLs
        try {
            final Request botReq = new Request.Builder()
                    .url(targetUrl)
                    .header("User-Agent", BOT_USER_AGENT)
                    .header("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
                    .header("Accept-Language", "en-US,en;q=0.9")
                    .build();

            try (Response botResp = client.newCall(botReq).execute()) {
                if (botResp.body() != null) {
                    final String html = botResp.body().string();
                    final StreamInfo info = parseFacebookEmbedHtml(client, originalUrl, html);
                    if (info != null) {
                        return info;
                    }
                }
            }
        } catch (final Exception e) {
            Log.w(TAG, "Facebook Crawler extraction failed", e);
        }

        // Method C: Direct Request with Browser User-Agent
        try {
            final Request pageReq = new Request.Builder()
                    .url(targetUrl)
                    .header("User-Agent", BROWSER_USER_AGENT)
                    .header("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
                    .header("Accept-Language", "en-US,en;q=0.9")
                    .build();

            try (Response pageResp = client.newCall(pageReq).execute()) {
                if (pageResp.body() != null) {
                    final String html = pageResp.body().string();
                    final StreamInfo info = parseFacebookEmbedHtml(client, originalUrl, html);
                    if (info != null) {
                        return info;
                    }
                }
            }
        } catch (final Exception ignored) { }

        return null;
    }

    @Nullable
    private static StreamInfo parseFacebookEmbedHtml(@Nullable final OkHttpClient client,
                                                     @NonNull final String originalUrl,
                                                     @NonNull final String html) {
        String hdUrl = null;
        String sdUrl = null;

        // 1. Look for hd_src / sd_src
        final Matcher hdMatcher = Pattern.compile("\"hd_src\"\\s*:\\s*\"([^\"]+)\"").matcher(html);
        if (hdMatcher.find()) {
            hdUrl = unescapeJsonString(hdMatcher.group(1));
        }

        final Matcher sdMatcher = Pattern.compile("\"sd_src\"\\s*:\\s*\"([^\"]+)\"").matcher(html);
        if (sdMatcher.find()) {
            sdUrl = unescapeJsonString(sdMatcher.group(1));
        }

        // 2. Look for browser_native_hd_url / browser_native_sd_url
        if (TextUtils.isEmpty(hdUrl)) {
            final Matcher m = Pattern.compile("\"browser_native_hd_url\"\\s*:\\s*\"([^\"]+)\"")
                    .matcher(html);
            if (m.find()) {
                hdUrl = unescapeJsonString(m.group(1));
            }
        }
        if (TextUtils.isEmpty(sdUrl)) {
            final Matcher m = Pattern.compile("\"browser_native_sd_url\"\\s*:\\s*\"([^\"]+)\"")
                    .matcher(html);
            if (m.find()) {
                sdUrl = unescapeJsonString(m.group(1));
            }
        }

        // 3. Look for playable_url_quality_hd / playable_url
        if (TextUtils.isEmpty(hdUrl)) {
            final Matcher m = Pattern.compile("\"playable_url_quality_hd\"\\s*:\\s*\"([^\"]+)\"")
                    .matcher(html);
            if (m.find()) {
                hdUrl = unescapeJsonString(m.group(1));
            }
        }
        if (TextUtils.isEmpty(sdUrl)) {
            final Matcher m = Pattern.compile("\"playable_url\"\\s*:\\s*\"([^\"]+)\"")
                    .matcher(html);
            if (m.find()) {
                sdUrl = unescapeJsonString(m.group(1));
            }
        }

        // 4. Look for base_url in Facebook video data
        if (TextUtils.isEmpty(hdUrl) && TextUtils.isEmpty(sdUrl)) {
            final Matcher m = Pattern.compile("\"base_url\"\\s*:\\s*\"([^\"]+fbcdn[^\"]+)\"").matcher(html);
            if (m.find()) {
                sdUrl = unescapeJsonString(m.group(1));
            }
        }

        // 5. Look for application/ld+json contentUrl
        if (TextUtils.isEmpty(sdUrl) && TextUtils.isEmpty(hdUrl)) {
            final Matcher cuMatcher = Pattern.compile("\"contentUrl\"\\s*:\\s*\"([^\"]+)\"").matcher(html);
            if (cuMatcher.find()) {
                sdUrl = unescapeJsonString(cuMatcher.group(1));
            }
        }

        // 6. Look for OpenGraph video
        if (TextUtils.isEmpty(sdUrl) && TextUtils.isEmpty(hdUrl)) {
            final Document doc = Jsoup.parse(html);
            final Element og = doc.selectFirst("meta[property=og:video], meta[property=og:video:url], "
                    + "meta[property=og:video:secure_url]");
            if (og != null && isValidMediaUrl(og.attr("content"))) {
                sdUrl = og.attr("content");
            }
        }

        if (TextUtils.isEmpty(hdUrl) && TextUtils.isEmpty(sdUrl)) {
            return null;
        }

        // Title resolution
        String title = "Facebook Video";
        final Document doc = Jsoup.parse(html);
        final Element ogTitle = doc.selectFirst("meta[property=og:title], meta[name=twitter:title]");
        if (ogTitle != null && !TextUtils.isEmpty(ogTitle.attr("content"))) {
            title = ogTitle.attr("content");
        } else if (!TextUtils.isEmpty(doc.title())) {
            title = doc.title().replace(" | Facebook", "").trim();
        }

        final List<VideoStream> videoStreams = new ArrayList<>();
        final List<AudioStream> audioStreams = new ArrayList<>();

        if (!TextUtils.isEmpty(hdUrl) && isValidMediaUrl(hdUrl)) {
            videoStreams.add(new VideoStream.Builder()
                    .setId("fb_hd")
                    .setContent(hdUrl, true)
                    .setMediaFormat(MediaFormat.MPEG_4)
                    .setResolution("1080p")
                    .setIsVideoOnly(false)
                    .setDeliveryMethod(DeliveryMethod.PROGRESSIVE_HTTP)
                    .build());

            audioStreams.add(new AudioStream.Builder()
                    .setId("fb_hd_audio")
                    .setContent(hdUrl, true)
                    .setMediaFormat(MediaFormat.M4A)
                    .setAverageBitrate(128)
                    .setDeliveryMethod(DeliveryMethod.PROGRESSIVE_HTTP)
                    .build());

            if (client != null) {
                probeMediaSize(client, hdUrl, "https://www.facebook.com/");
            }
        }

        if (!TextUtils.isEmpty(sdUrl) && isValidMediaUrl(sdUrl)) {
            videoStreams.add(new VideoStream.Builder()
                    .setId("fb_sd")
                    .setContent(sdUrl, true)
                    .setMediaFormat(MediaFormat.MPEG_4)
                    .setResolution("720p")
                    .setIsVideoOnly(false)
                    .setDeliveryMethod(DeliveryMethod.PROGRESSIVE_HTTP)
                    .build());

            if (audioStreams.isEmpty()) {
                audioStreams.add(new AudioStream.Builder()
                        .setId("fb_sd_audio")
                        .setContent(sdUrl, true)
                        .setMediaFormat(MediaFormat.M4A)
                        .setAverageBitrate(128)
                        .setDeliveryMethod(DeliveryMethod.PROGRESSIVE_HTTP)
                        .build());
            }

            if (client != null) {
                probeMediaSize(client, sdUrl, "https://www.facebook.com/");
            }
        }

        if (videoStreams.isEmpty()) {
            return null;
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
        streamInfo.setAudioStreams(audioStreams);
        streamInfo.setSubtitles(Collections.emptyList());
        return streamInfo;
    }

    /**
     * Extracts videos from Instagram Reels and Posts.
     */
    @Nullable
    private static StreamInfo extractInstagram(@NonNull final OkHttpClient client,
                                               @NonNull final String originalUrl) {
        String targetUrl = originalUrl;
        if (targetUrl.contains("/share/")) {
            try {
                final Request shareReq = new Request.Builder()
                        .url(targetUrl)
                        .header("User-Agent", MOBILE_USER_AGENT)
                        .build();
                try (Response shareResp = client.newCall(shareReq).execute()) {
                    targetUrl = shareResp.request().url().toString();
                }
            } catch (final Exception ignored) { }
        }

        String cleanUrl = targetUrl.split("\\?")[0];
        if (cleanUrl.endsWith("/")) {
            cleanUrl = cleanUrl.substring(0, cleanUrl.length() - 1);
        }

        // Method A: Direct page request with browser User-Agent
        try {
            final Request pageReq = new Request.Builder()
                    .url(cleanUrl + "/")
                    .header("User-Agent", BROWSER_USER_AGENT)
                    .header("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
                    .header("Accept-Language", "en-US,en;q=0.9")
                    .build();

            try (Response pageResp = client.newCall(pageReq).execute()) {
                if (pageResp.body() != null) {
                    final String html = pageResp.body().string();
                    final StreamInfo info = parseInstagramHtml(client, originalUrl, html);
                    if (info != null) {
                        return info;
                    }
                }
            }
        } catch (final Exception ignored) { }

        // Method B: Instagram Embed captioned
        try {
            final String embedUrl = cleanUrl + "/embed/captioned/";
            final Request req = new Request.Builder()
                    .url(embedUrl)
                    .header("User-Agent", BROWSER_USER_AGENT)
                    .header("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
                    .build();

            try (Response resp = client.newCall(req).execute()) {
                if (resp.body() != null) {
                    final String html = resp.body().string();
                    final StreamInfo info = parseInstagramHtml(client, originalUrl, html);
                    if (info != null) {
                        return info;
                    }
                }
            }
        } catch (final Exception ignored) { }

        // Method C: Instagram Embed standard
        try {
            final String embedUrl = cleanUrl + "/embed/";
            final Request req = new Request.Builder()
                    .url(embedUrl)
                    .header("User-Agent", BROWSER_USER_AGENT)
                    .header("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
                    .build();

            try (Response resp = client.newCall(req).execute()) {
                if (resp.body() != null) {
                    final String html = resp.body().string();
                    final StreamInfo info = parseInstagramHtml(client, originalUrl, html);
                    if (info != null) {
                        return info;
                    }
                }
            }
        } catch (final Exception ignored) { }

        // Method D: Crawler User-Agent
        try {
            final Request botReq = new Request.Builder()
                    .url(targetUrl)
                    .header("User-Agent", BOT_USER_AGENT)
                    .header("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
                    .build();

            try (Response botResp = client.newCall(botReq).execute()) {
                if (botResp.body() != null) {
                    final String html = botResp.body().string();
                    final StreamInfo info = parseInstagramHtml(client, originalUrl, html);
                    if (info != null) {
                        return info;
                    }
                }
            }
        } catch (final Exception ignored) { }

        return null;
    }

    @Nullable
    private static StreamInfo parseInstagramHtml(@NonNull final OkHttpClient client,
                                                 @NonNull final String originalUrl,
                                                 @NonNull final String html) {
        final Document doc = Jsoup.parse(html);
        String title = "Instagram Reel";
        final Element ogTitle = doc.selectFirst("meta[property=og:title], meta[name=twitter:title]");
        if (ogTitle != null && !TextUtils.isEmpty(ogTitle.attr("content"))) {
            title = ogTitle.attr("content");
        } else if (!TextUtils.isEmpty(doc.title())) {
            title = doc.title().replace(" | Instagram", "").trim();
        }

        // 1. Check application/ld+json
        for (final Element script : doc.select("script[type=application/ld+json]")) {
            try {
                final String jsonStr = script.data();
                if (jsonStr.contains("contentUrl")) {
                    final Matcher cuMatcher = Pattern.compile("\"contentUrl\"\\s*:\\s*\"([^\"]+)\"").matcher(jsonStr);
                    if (cuMatcher.find()) {
                        final String vUrl = unescapeJsonString(cuMatcher.group(1));
                        if (isValidMediaUrl(vUrl)) {
                            probeMediaSize(client, vUrl, "https://www.instagram.com/");
                            return createSingleStreamInfo(originalUrl, title, vUrl, MediaFormat.MPEG_4);
                        }
                    }
                }
            } catch (final Exception ignored) { }
        }

        // 2. Check video_url regex
        final Matcher videoUrlMatcher = Pattern.compile(
                "\"video_url\"\\s*:\\s*\"([^\"]+)\"").matcher(html);
        if (videoUrlMatcher.find()) {
            final String videoUrl = unescapeJsonString(videoUrlMatcher.group(1));
            if (isValidMediaUrl(videoUrl)) {
                probeMediaSize(client, videoUrl, "https://www.instagram.com/");
                return createSingleStreamInfo(originalUrl, title, videoUrl,
                        MediaFormat.MPEG_4);
            }
        }

        // 3. Check video_versions array
        final Matcher versionsMatcher = Pattern.compile(
                "\"video_versions\"\\s*:\\s*\\[\\s*\\{[^}]*\"url\"\\s*:\\s*\"([^\"]+)\"").matcher(html);
        if (versionsMatcher.find()) {
            final String videoUrl = unescapeJsonString(versionsMatcher.group(1));
            if (isValidMediaUrl(videoUrl)) {
                probeMediaSize(client, videoUrl, "https://www.instagram.com/");
                return createSingleStreamInfo(originalUrl, title, videoUrl,
                        MediaFormat.MPEG_4);
            }
        }

        // 4. Check OpenGraph video
        final Element ogVideo = doc.selectFirst("meta[property=og:video], "
                + "meta[property=og:video:secure_url], meta[property=og:video:url]");
        if (ogVideo != null && isValidMediaUrl(ogVideo.attr("content"))) {
            final String src = ogVideo.attr("content");
            probeMediaSize(client, src, "https://www.instagram.com/");
            return createSingleStreamInfo(originalUrl, title, src, MediaFormat.MPEG_4);
        }

        // 5. Check HTML5 Video tags
        final Element videoElem = doc.selectFirst("video.EmbeddedVideo, video[src]");
        if (videoElem != null) {
            final String src = videoElem.attr("src");
            if (isValidMediaUrl(src)) {
                probeMediaSize(client, src, "https://www.instagram.com/");
                return createSingleStreamInfo(originalUrl, title, src,
                        MediaFormat.MPEG_4);
            }
        }

        return null;
    }

    /**
     * Extracts videos from TikTok using TikWM API and fallback script parsing.
     */
    @Nullable
    private static StreamInfo extractTikTok(@NonNull final OkHttpClient client,
                                            @NonNull final String originalUrl) {
        // Resolve shortened TikTok URLs (vt.tiktok.com, vm.tiktok.com)
        String canonicalUrl = originalUrl;
        if (canonicalUrl.contains("vt.tiktok.com") || canonicalUrl.contains("vm.tiktok.com")) {
            try {
                final Request r = new Request.Builder()
                        .url(canonicalUrl)
                        .header("User-Agent", MOBILE_USER_AGENT)
                        .build();
                try (Response resp = client.newCall(r).execute()) {
                    canonicalUrl = resp.request().url().toString();
                }
            } catch (final Exception e) {
                Log.w(TAG, "Failed resolving TikTok short link", e);
            }
        }

        // Method A: TikWM API (High-quality, direct MP4 without watermark)
        try {
            final String encoded = URLEncoder.encode(canonicalUrl, StandardCharsets.UTF_8.name());
            final Request apiReq = new Request.Builder()
                    .url("https://www.tikwm.com/api/?url=" + encoded)
                    .header("User-Agent", BROWSER_USER_AGENT)
                    .build();

            try (Response apiResp = client.newCall(apiReq).execute()) {
                if (apiResp.isSuccessful() && apiResp.body() != null) {
                    final JSONObject json = new JSONObject(apiResp.body().string());
                    if (json.optInt("code") == 0 && json.has("data")) {
                        final JSONObject data = json.getJSONObject("data");
                        final String title = data.optString("title", "TikTok Video");
                        final String playUrl = data.optString("play");
                        final String hdPlayUrl = data.optString("hdplay");
                        final String chosenUrl = !TextUtils.isEmpty(hdPlayUrl) ? hdPlayUrl : playUrl;

                        if (isValidMediaUrl(chosenUrl)) {
                            final long tikSize = data.optLong("hd_size", 0) > 0
                                    ? data.optLong("hd_size") : data.optLong("size", 0);
                            if (tikSize > 0) {
                                cacheSize(chosenUrl, tikSize);
                            } else {
                                probeMediaSize(client, chosenUrl, "https://www.tiktok.com/");
                            }

                            final StreamInfo streamInfo = createSingleStreamInfo(originalUrl,
                                    title, chosenUrl, MediaFormat.MPEG_4);

                            final List<AudioStream> audioList = new ArrayList<>(streamInfo.getAudioStreams());
                            final String musicUrl = data.optString("music");
                            if (isValidMediaUrl(musicUrl)) {
                                probeMediaSize(client, musicUrl, "https://www.tiktok.com/");
                                audioList.add(0, new AudioStream.Builder()
                                        .setId("tiktok_music")
                                        .setContent(musicUrl, true)
                                        .setMediaFormat(MediaFormat.MP3)
                                        .setAverageBitrate(192)
                                        .setDeliveryMethod(DeliveryMethod.PROGRESSIVE_HTTP)
                                        .build());
                            }
                            streamInfo.setAudioStreams(audioList);
                            return streamInfo;
                        }
                    }
                }
            }
        } catch (final Exception e) {
            Log.w(TAG, "TikWM API extraction failed", e);
        }

        // Method B: Embedded script extraction fallback
        try {
            final Request htmlReq = new Request.Builder()
                    .url(canonicalUrl)
                    .header("User-Agent", BROWSER_USER_AGENT)
                    .build();

            try (Response htmlResp = client.newCall(htmlReq).execute()) {
                if (htmlResp.body() != null) {
                    final String html = htmlResp.body().string();
                    final Document doc = Jsoup.parse(html, canonicalUrl);
                    final Element universalData = doc.getElementById(
                            "__UNIVERSAL_DATA_FOR_REHYDRATION__");
                    if (universalData != null) {
                        final JSONObject json = new JSONObject(universalData.data());
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

                                if (isValidMediaUrl(chosenUrl)) {
                                    probeMediaSize(client, chosenUrl, "https://www.tiktok.com/");
                                    return createSingleStreamInfo(originalUrl, desc, chosenUrl,
                                            MediaFormat.MPEG_4);
                                }
                            }
                        }
                    }
                }
            }
        } catch (final Exception ignored) { }

        // Method C: OpenGraph Video / HTML5 Video tags fallback
        try {
            final Request htmlReq = new Request.Builder()
                    .url(canonicalUrl)
                    .header("User-Agent", BOT_USER_AGENT)
                    .build();

            try (Response htmlResp = client.newCall(htmlReq).execute()) {
                if (htmlResp.body() != null) {
                    final String html = htmlResp.body().string();
                    final Document doc = Jsoup.parse(html, canonicalUrl);
                    final Element ogVideo = doc.selectFirst(
                            "meta[property=og:video:secure_url], meta[property=og:video]");
                    if (ogVideo != null && isValidMediaUrl(ogVideo.attr("content"))) {
                        final String playUrl = ogVideo.attr("content");
                        probeMediaSize(client, playUrl, "https://www.tiktok.com/");
                        final String title = !TextUtils.isEmpty(doc.title())
                                ? doc.title().replace(" | TikTok", "") : "TikTok Video";
                        return createSingleStreamInfo(originalUrl, title, playUrl, MediaFormat.MPEG_4);
                    }
                }
            }
        } catch (final Exception ignored) { }

        return null;
    }

    /**
     * Extracts videos from Twitter / X status posts using FxTwitter and Syndication APIs.
     */
    @Nullable
    private static StreamInfo extractTwitter(@NonNull final OkHttpClient client,
                                             @NonNull final String originalUrl) {
        final Matcher matcher = Pattern.compile("status/(\\d+)").matcher(originalUrl);
        if (!matcher.find()) {
            return null;
        }
        final String tweetId = matcher.group(1);

        // Method A: FxTwitter / FixTweet API
        try {
            final Request fxReq = new Request.Builder()
                    .url("https://api.fxtwitter.com/i/status/" + tweetId)
                    .header("User-Agent", BROWSER_USER_AGENT)
                    .build();

            try (Response fxResp = client.newCall(fxReq).execute()) {
                if (fxResp.isSuccessful() && fxResp.body() != null) {
                    final JSONObject json = new JSONObject(fxResp.body().string());
                    final JSONObject tweet = json.optJSONObject("tweet");
                    if (tweet != null) {
                        final String title = tweet.optString("text", "X Video");
                        final JSONObject media = tweet.optJSONObject("media");
                        String videoUrl = null;
                        if (media != null) {
                            if (media.has("videos")) {
                                final JSONArray videos = media.getJSONArray("videos");
                                if (videos.length() > 0) {
                                    videoUrl = videos.getJSONObject(0).optString("url");
                                }
                            }
                            if (TextUtils.isEmpty(videoUrl) && media.has("all")) {
                                final JSONArray all = media.getJSONArray("all");
                                for (int i = 0; i < all.length(); i++) {
                                    final JSONObject item = all.getJSONObject(i);
                                    final String type = item.optString("type");
                                    if ("video".equalsIgnoreCase(type) || "gif".equalsIgnoreCase(type)) {
                                        videoUrl = item.optString("url");
                                        break;
                                    }
                                }
                            }
                        }
                        if (isValidMediaUrl(videoUrl)) {
                            probeMediaSize(client, videoUrl, "https://twitter.com/");
                            return createSingleStreamInfo(originalUrl, title, videoUrl,
                                    MediaFormat.MPEG_4);
                        }
                    }
                }
            }
        } catch (final Exception e) {
            Log.w(TAG, "FxTwitter extraction failed", e);
        }

        // Method B: VxTwitter API fallback
        try {
            final Request vxReq = new Request.Builder()
                    .url("https://api.vxtwitter.com/Twitter/status/" + tweetId)
                    .header("User-Agent", BROWSER_USER_AGENT)
                    .build();

            try (Response vxResp = client.newCall(vxReq).execute()) {
                if (vxResp.isSuccessful() && vxResp.body() != null) {
                    final JSONObject json = new JSONObject(vxResp.body().string());
                    final String title = json.optString("text", "X Video");
                    String videoUrl = null;
                    if (json.has("media_extended")) {
                        final JSONArray ext = json.getJSONArray("media_extended");
                        for (int i = 0; i < ext.length(); i++) {
                            final JSONObject item = ext.getJSONObject(i);
                            final String type = item.optString("type");
                            if ("video".equalsIgnoreCase(type) || "gif".equalsIgnoreCase(type)) {
                                videoUrl = item.optString("url");
                                break;
                            }
                        }
                    }
                    if (TextUtils.isEmpty(videoUrl) && json.has("mediaURLs")) {
                        final JSONArray urls = json.getJSONArray("mediaURLs");
                        for (int i = 0; i < urls.length(); i++) {
                            final String u = urls.getString(i);
                            if (u.contains(".mp4") || u.contains("video.twimg.com")) {
                                videoUrl = u;
                                break;
                            }
                        }
                    }
                    if (TextUtils.isEmpty(videoUrl)) {
                        final JSONObject media = json.optJSONObject("media");
                        if (media != null && media.has("videos")) {
                            final JSONArray videos = media.getJSONArray("videos");
                            if (videos.length() > 0) {
                                videoUrl = videos.getJSONObject(0).optString("url");
                            }
                        }
                    }
                    if (isValidMediaUrl(videoUrl)) {
                        probeMediaSize(client, videoUrl, "https://twitter.com/");
                        return createSingleStreamInfo(originalUrl, title, videoUrl,
                                MediaFormat.MPEG_4);
                    }
                }
            }
        } catch (final Exception ignored) { }

        // Method C: Twitter Syndication API fallback
        try {
            final Request synReq = new Request.Builder()
                    .url("https://cdn.syndication.twimg.com/tweet-result?id=" + tweetId + "&token=x")
                    .header("User-Agent", BROWSER_USER_AGENT)
                    .build();

            try (Response synResp = client.newCall(synReq).execute()) {
                if (synResp.isSuccessful() && synResp.body() != null) {
                    final JSONObject json = new JSONObject(synResp.body().string());
                    final String text = json.optString("text", "X Video");
                    final JSONObject videoObj = json.optJSONObject("video");
                    if (videoObj != null && videoObj.has("variants")) {
                        final JSONArray variants = videoObj.getJSONArray("variants");
                        String bestUrl = null;
                        int maxBitrate = -1;
                        for (int i = 0; i < variants.length(); i++) {
                            final JSONObject v = variants.getJSONObject(i);
                            final String vUrl = v.optString("src");
                            final int bitrate = v.optInt("bitrate", 0);
                            if (vUrl.contains(".mp4") && bitrate >= maxBitrate) {
                                maxBitrate = bitrate;
                                bestUrl = vUrl;
                            }
                        }
                        if (isValidMediaUrl(bestUrl)) {
                            probeMediaSize(client, bestUrl, "https://twitter.com/");
                            return createSingleStreamInfo(originalUrl, text, bestUrl,
                                    MediaFormat.MPEG_4);
                        }
                    }
                }
            }
        } catch (final Exception e) {
            Log.w(TAG, "Twitter Syndication extraction failed", e);
        }

        return null;
    }

    /**
     * Extracts videos and audio from Reddit posts.
     */
    @Nullable
    private static StreamInfo extractReddit(@NonNull final OkHttpClient client,
                                            @NonNull final String originalUrl) {
        String cleanUrl = originalUrl.split("\\?")[0];
        if (!cleanUrl.endsWith(".json")) {
            cleanUrl = cleanUrl + ".json";
        }

        try {
            final Request req = new Request.Builder()
                    .url(cleanUrl)
                    .header("User-Agent", BROWSER_USER_AGENT)
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
                        final JSONObject mediaObj = postObj.optJSONObject("secure_media") != null
                                && postObj.getJSONObject("secure_media").optJSONObject("reddit_video") != null
                                ? postObj.getJSONObject("secure_media").getJSONObject("reddit_video")
                                : (postObj.optJSONObject("media") != null
                                ? postObj.getJSONObject("media").optJSONObject("reddit_video") : null);

                        if (mediaObj != null) {
                            final String fallbackUrl = mediaObj.optString("fallback_url");
                            final String hlsUrl = mediaObj.optString("hls_url");
                            final boolean isGif = mediaObj.optBoolean("is_gif", false);
                            if (!TextUtils.isEmpty(fallbackUrl)) {
                                probeMediaSize(client, fallbackUrl, "https://www.reddit.com/");
                                return buildRedditStreamInfo(originalUrl, title, fallbackUrl, hlsUrl, isGif);
                            }
                        }
                    }
                }
            }
        } catch (final Exception e) {
            Log.w(TAG, "Reddit JSON extraction failed", e);
        }

        return null;
    }

    @NonNull
    private static StreamInfo buildRedditStreamInfo(@NonNull final String originalUrl,
                                                    @NonNull final String title,
                                                    @NonNull final String fallbackUrl,
                                                    @Nullable final String hlsUrl,
                                                    final boolean isGif) {
        final StreamInfo streamInfo = new StreamInfo(
                ServiceList.MediaCCC.getServiceId(),
                originalUrl,
                originalUrl,
                StreamType.VIDEO_STREAM,
                String.valueOf(originalUrl.hashCode()),
                title,
                0
        );

        if (!TextUtils.isEmpty(hlsUrl)) {
            streamInfo.setHlsUrl(hlsUrl);
        }

        final List<VideoStream> videoStreams = new ArrayList<>();
        final List<VideoStream> videoOnlyStreams = new ArrayList<>();
        final String queryParams = fallbackUrl.contains("?")
                ? fallbackUrl.substring(fallbackUrl.indexOf('?')) : "";
        final String cleanFallback = fallbackUrl.split("\\?")[0];
        final String baseUrl = cleanFallback.substring(0, cleanFallback.lastIndexOf('/'));

        if (!TextUtils.isEmpty(fallbackUrl)) {
            videoStreams.add(new VideoStream.Builder()
                    .setId("reddit_progressive")
                    .setContent(fallbackUrl, true)
                    .setMediaFormat(MediaFormat.MPEG_4)
                    .setResolution("720p")
                    .setIsVideoOnly(false)
                    .setDeliveryMethod(DeliveryMethod.PROGRESSIVE_HTTP)
                    .build());
        }

        if (!TextUtils.isEmpty(hlsUrl)) {
            videoStreams.add(new VideoStream.Builder()
                    .setId("reddit_hls")
                    .setContent(hlsUrl, true)
                    .setMediaFormat(MediaFormat.MPEG_4)
                    .setResolution("1080p")
                    .setIsVideoOnly(false)
                    .setDeliveryMethod(DeliveryMethod.HLS)
                    .build());
        }

        videoOnlyStreams.add(new VideoStream.Builder()
                .setId("reddit_fallback")
                .setContent(fallbackUrl, true)
                .setMediaFormat(MediaFormat.MPEG_4)
                .setResolution("720p")
                .setIsVideoOnly(true)
                .setDeliveryMethod(DeliveryMethod.PROGRESSIVE_HTTP)
                .build());

        final String[] resolutions = {"1080", "720", "480", "360", "240"};
        for (final String res : resolutions) {
            final String streamUrl = baseUrl + "/DASH_" + res + ".mp4" + queryParams;
            videoOnlyStreams.add(new VideoStream.Builder()
                    .setId("reddit_v_" + res)
                    .setContent(streamUrl, true)
                    .setMediaFormat(MediaFormat.MPEG_4)
                    .setResolution(res + "p")
                    .setIsVideoOnly(true)
                    .setDeliveryMethod(DeliveryMethod.PROGRESSIVE_HTTP)
                    .build());
        }

        final List<AudioStream> audioStreams = new ArrayList<>();
        if (!isGif) {
            final String audio128Url = baseUrl + "/DASH_AUDIO_128.mp4" + queryParams;
            final String audioLegacyUrl = baseUrl + "/DASH_audio.mp4" + queryParams;

            audioStreams.add(new AudioStream.Builder()
                    .setId("reddit_a_128")
                    .setContent(audio128Url, true)
                    .setMediaFormat(MediaFormat.M4A)
                    .setAverageBitrate(128)
                    .setDeliveryMethod(DeliveryMethod.PROGRESSIVE_HTTP)
                    .build());

            audioStreams.add(new AudioStream.Builder()
                    .setId("reddit_a_legacy")
                    .setContent(audioLegacyUrl, true)
                    .setMediaFormat(MediaFormat.M4A)
                    .setAverageBitrate(96)
                    .setDeliveryMethod(DeliveryMethod.PROGRESSIVE_HTTP)
                    .build());
        }

        streamInfo.setVideoStreams(videoStreams);
        streamInfo.setVideoOnlyStreams(videoOnlyStreams);
        streamInfo.setAudioStreams(audioStreams);
        streamInfo.setSubtitles(Collections.emptyList());
        return streamInfo;
    }

    /*//////////////////////////////////////////////////////////////////////////
    // Generic HTML / Direct Media
    //////////////////////////////////////////////////////////////////////////*/

    @Nullable
    private static StreamInfo extractGenericHtml(@Nullable final OkHttpClient client,
                                                 @NonNull final String originalUrl,
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

        // 5. Script Regex search for direct MP4 and M3U8
        if (candidates.isEmpty()) {
            final Matcher mp4Matcher = Pattern.compile(
                    "(https?:[\\\\/]+[^\"'\\s]+\\.(?:mp4|m3u8)[^\"'\\s]*)").matcher(html);
            while (mp4Matcher.find()) {
                final String foundUrl = unescapeJsonString(mp4Matcher.group(1));
                if (isValidMediaUrl(foundUrl)) {
                    candidates.add(foundUrl);
                }
            }
        }

        if (candidates.isEmpty()) {
            return null;
        }

        final List<VideoStream> videoStreams = new ArrayList<>();
        final List<AudioStream> audioStreams = new ArrayList<>();
        int index = 0;
        for (final String streamUrl : candidates) {
            final boolean isHls = streamUrl.contains(".m3u8");
            final MediaFormat vFormat = isHls ? MediaFormat.MPEG_4 : resolveVideoFormat(streamUrl, null);
            final VideoStream stream = new VideoStream.Builder()
                    .setId("video_" + index)
                    .setContent(streamUrl, true)
                    .setMediaFormat(vFormat)
                    .setResolution("1080p")
                    .setIsVideoOnly(false)
                    .setDeliveryMethod(isHls ? DeliveryMethod.HLS : DeliveryMethod.PROGRESSIVE_HTTP)
                    .build();
            videoStreams.add(stream);

            if (client != null) {
                probeMediaSize(client, streamUrl, originalUrl);
            }

            if (!isHls && vFormat == MediaFormat.MPEG_4) {
                audioStreams.add(new AudioStream.Builder()
                        .setId("audio_" + index)
                        .setContent(streamUrl, true)
                        .setMediaFormat(MediaFormat.M4A)
                        .setAverageBitrate(128)
                        .setDeliveryMethod(DeliveryMethod.PROGRESSIVE_HTTP)
                        .build());
            }
            index++;
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
        streamInfo.setAudioStreams(audioStreams);
        streamInfo.setSubtitles(Collections.emptyList());
        return streamInfo;
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
                    .setResolution("1080p")
                    .setIsVideoOnly(false)
                    .setDeliveryMethod(isHls ? DeliveryMethod.HLS : DeliveryMethod.PROGRESSIVE_HTTP)
                    .build();
            streamInfo.setVideoStreams(Collections.singletonList(videoStream));

            final List<AudioStream> audioList = new ArrayList<>();
            if (format == MediaFormat.MPEG_4) {
                audioList.add(new AudioStream.Builder()
                        .setId("audio_0")
                        .setContent(mediaUrl, true)
                        .setMediaFormat(MediaFormat.M4A)
                        .setAverageBitrate(128)
                        .setDeliveryMethod(isHls ? DeliveryMethod.HLS : DeliveryMethod.PROGRESSIVE_HTTP)
                        .build());
            }
            streamInfo.setAudioStreams(audioList);
            streamInfo.setVideoOnlyStreams(Collections.emptyList());
        }

        streamInfo.setSubtitles(Collections.emptyList());
        return streamInfo;
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

        final boolean isHls = videoUrl.contains(".m3u8");
        final VideoStream videoStream = new VideoStream.Builder()
                .setId("video_0")
                .setContent(videoUrl, true)
                .setMediaFormat(format)
                .setResolution("1080p")
                .setIsVideoOnly(false)
                .setDeliveryMethod(isHls ? DeliveryMethod.HLS : DeliveryMethod.PROGRESSIVE_HTTP)
                .build();

        final List<AudioStream> audioStreams = new ArrayList<>();
        if (format == MediaFormat.MPEG_4) {
            audioStreams.add(new AudioStream.Builder()
                    .setId("audio_0")
                    .setContent(videoUrl, true)
                    .setMediaFormat(MediaFormat.M4A)
                    .setAverageBitrate(128)
                    .setDeliveryMethod(isHls ? DeliveryMethod.HLS : DeliveryMethod.PROGRESSIVE_HTTP)
                    .build());
        }

        streamInfo.setVideoStreams(Collections.singletonList(videoStream));
        streamInfo.setVideoOnlyStreams(Collections.emptyList());
        streamInfo.setAudioStreams(audioStreams);
        streamInfo.setSubtitles(Collections.emptyList());
        return streamInfo;
    }

    /**
     * Proactively probes content size using HEAD request and falls back to a 1-byte GET Range request.
     */
    public static long probeMediaSize(@NonNull final OkHttpClient client,
                                      @NonNull final String mediaUrl,
                                      @Nullable final String referer) {
        if (TextUtils.isEmpty(mediaUrl)) {
            return -1;
        }
        final long cached = getCachedSize(mediaUrl);
        if (cached > 0) {
            return cached;
        }

        // 1. Try HEAD request with browser headers
        try {
            final Request.Builder rb = new Request.Builder()
                    .url(mediaUrl)
                    .head()
                    .header("User-Agent", BROWSER_USER_AGENT)
                    .header("Accept", "*/*");
            if (!TextUtils.isEmpty(referer)) {
                rb.header("Referer", referer);
            }
            try (Response resp = client.newCall(rb.build()).execute()) {
                if (resp.isSuccessful()) {
                    final String cl = resp.header("Content-Length");
                    if (!TextUtils.isEmpty(cl)) {
                        try {
                            final long len = Long.parseLong(cl);
                            if (len > 0) {
                                final String finalUrl = resp.request().url().toString();
                                cacheSize(mediaUrl, len);
                                if (!finalUrl.equals(mediaUrl)) {
                                    cacheSize(finalUrl, len);
                                }
                                return len;
                            }
                        } catch (final NumberFormatException ignored) { }
                    }
                }
            }
        } catch (final Exception ignored) { }

        // 2. Try GET request with Range: bytes=0-0
        try {
            final Request.Builder rb = new Request.Builder()
                    .url(mediaUrl)
                    .get()
                    .header("User-Agent", BROWSER_USER_AGENT)
                    .header("Range", "bytes=0-0")
                    .header("Accept", "*/*");
            if (!TextUtils.isEmpty(referer)) {
                rb.header("Referer", referer);
            }
            try (Response resp = client.newCall(rb.build()).execute()) {
                final String finalUrl = resp.request().url().toString();
                final String cr = resp.header("Content-Range");
                if (!TextUtils.isEmpty(cr) && cr.contains("/")) {
                    final String totalStr = cr.substring(cr.lastIndexOf('/') + 1).trim();
                    try {
                        final long len = Long.parseLong(totalStr);
                        if (len > 0) {
                            cacheSize(mediaUrl, len);
                            if (!finalUrl.equals(mediaUrl)) {
                                cacheSize(finalUrl, len);
                            }
                            return len;
                        }
                    } catch (final NumberFormatException ignored) { }
                }
                if (resp.isSuccessful()) {
                    final String cl = resp.header("Content-Length");
                    if (!TextUtils.isEmpty(cl)) {
                        try {
                            final long len = Long.parseLong(cl);
                            if (len > 0) {
                                cacheSize(mediaUrl, len);
                                if (!finalUrl.equals(mediaUrl)) {
                                    cacheSize(finalUrl, len);
                                }
                                return len;
                            }
                        } catch (final NumberFormatException ignored) { }
                    }
                }
            }
        } catch (final Exception ignored) { }

        return -1;
    }

    /*//////////////////////////////////////////////////////////////////////////
    // Utilities
    //////////////////////////////////////////////////////////////////////////*/

    private static boolean isValidMediaUrl(@Nullable final String url) {
        if (TextUtils.isEmpty(url)) {
            return false;
        }
        final String lower = url.toLowerCase(Locale.ROOT);
        return lower.startsWith("http://") || lower.startsWith("https://");
    }

    @NonNull
    private static String unescapeJsonString(@NonNull final String input) {
        String result = input.replace("\\/", "/");
        if (result.contains("\\u")) {
            final Matcher matcher = UNICODE_ESCAPE_PATTERN.matcher(result);
            final StringBuffer sb = new StringBuffer();
            while (matcher.find()) {
                try {
                    final int code = Integer.parseInt(matcher.group(1), 16);
                    matcher.appendReplacement(sb, Matcher.quoteReplacement(String.valueOf((char) code)));
                } catch (final NumberFormatException e) {
                    matcher.appendReplacement(sb, Matcher.quoteReplacement(matcher.group(0)));
                }
            }
            matcher.appendTail(sb);
            result = sb.toString();
        }
        return result.replace("&amp;", "&");
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
