package org.schabi.newpipe.util;

import android.util.Log;

import org.schabi.newpipe.extractor.MediaFormat;
import org.schabi.newpipe.extractor.services.youtube.ItagItem;
import org.schabi.newpipe.extractor.services.youtube.ItagItem.ItagType;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Registers extended YouTube itags (8K, 4K 60fps, VP9.2 HDR, and AV1 HDR) into NewPipeExtractor's
 * {@link ItagItem#ITAG_LIST} dynamically at runtime so that NewPipe can extract, play, and
 * download ultra-high-definition and HDR video streams.
 */
public final class ItagHelper {
    private static final String TAG = "ItagHelper";
    private static volatile boolean initialized = false;

    private ItagHelper() { }

    /**
     * Initializes and registers the extended 8K and HDR itags into ItagItem.
     */
    public static synchronized void initExtendedItags() {
        if (initialized) {
            return;
        }

        try {
            final Field itagListField = ItagItem.class.getDeclaredField("ITAG_LIST");
            itagListField.setAccessible(true);
            final ItagItem[] originalList = (ItagItem[]) itagListField.get(null);
            if (originalList == null) {
                return;
            }

            final Set<Integer> existingIds = new HashSet<>();
            for (final ItagItem item : originalList) {
                existingIds.add(item.id);
            }

            final List<ItagItem> updatedList = new ArrayList<>(Arrays.asList(originalList));

            // Define extended 8K and HDR itags
            final ItagItem[] extendedItems = new ItagItem[]{
                    // 8K Video Only (AV1 / VP9 / MP4)
                    new ItagItem(571, ItagType.VIDEO_ONLY, MediaFormat.MPEG_4, "4320p60", 60),
                    new ItagItem(402, ItagType.VIDEO_ONLY, MediaFormat.MPEG_4, "4320p", 30),
                    new ItagItem(272, ItagType.VIDEO_ONLY, MediaFormat.WEBM, "4320p", 30),
                    new ItagItem(138, ItagType.VIDEO_ONLY, MediaFormat.MPEG_4, "4320p", 30),

                    // AV1 SDR Video Only (144p - 2160p)
                    new ItagItem(401, ItagType.VIDEO_ONLY, MediaFormat.MPEG_4, "2160p60", 60),
                    new ItagItem(400, ItagType.VIDEO_ONLY, MediaFormat.MPEG_4, "1440p60", 60),
                    new ItagItem(399, ItagType.VIDEO_ONLY, MediaFormat.MPEG_4, "1080p60", 60),
                    new ItagItem(398, ItagType.VIDEO_ONLY, MediaFormat.MPEG_4, "720p60", 60),
                    new ItagItem(397, ItagType.VIDEO_ONLY, MediaFormat.MPEG_4, "480p", 30),
                    new ItagItem(396, ItagType.VIDEO_ONLY, MediaFormat.MPEG_4, "360p", 30),
                    new ItagItem(395, ItagType.VIDEO_ONLY, MediaFormat.MPEG_4, "240p", 30),
                    new ItagItem(394, ItagType.VIDEO_ONLY, MediaFormat.MPEG_4, "144p", 30),

                    // HDR VP9.2 (WebM)
                    new ItagItem(337, ItagType.VIDEO_ONLY, MediaFormat.WEBM, "2160p60 HDR", 60),
                    new ItagItem(336, ItagType.VIDEO_ONLY, MediaFormat.WEBM, "1440p60 HDR", 60),
                    new ItagItem(335, ItagType.VIDEO_ONLY, MediaFormat.WEBM, "1080p60 HDR", 60),
                    new ItagItem(334, ItagType.VIDEO_ONLY, MediaFormat.WEBM, "720p60 HDR", 60),
                    new ItagItem(333, ItagType.VIDEO_ONLY, MediaFormat.WEBM, "480p HDR", 30),
                    new ItagItem(332, ItagType.VIDEO_ONLY, MediaFormat.WEBM, "360p HDR", 30),
                    new ItagItem(331, ItagType.VIDEO_ONLY, MediaFormat.WEBM, "240p HDR", 30),
                    new ItagItem(330, ItagType.VIDEO_ONLY, MediaFormat.WEBM, "144p HDR", 30),

                    // HDR AV1
                    new ItagItem(702, ItagType.VIDEO_ONLY, MediaFormat.MPEG_4, "4320p60 HDR", 60),
                    new ItagItem(701, ItagType.VIDEO_ONLY, MediaFormat.MPEG_4, "2160p60 HDR", 60),
                    new ItagItem(700, ItagType.VIDEO_ONLY, MediaFormat.MPEG_4, "1440p60 HDR", 60),
                    new ItagItem(699, ItagType.VIDEO_ONLY, MediaFormat.MPEG_4, "1080p60 HDR", 60),
                    new ItagItem(698, ItagType.VIDEO_ONLY, MediaFormat.MPEG_4, "720p60 HDR", 60),
                    new ItagItem(697, ItagType.VIDEO_ONLY, MediaFormat.MPEG_4, "480p HDR", 30),
                    new ItagItem(696, ItagType.VIDEO_ONLY, MediaFormat.MPEG_4, "360p HDR", 30),
                    new ItagItem(695, ItagType.VIDEO_ONLY, MediaFormat.MPEG_4, "240p HDR", 30),
                    new ItagItem(694, ItagType.VIDEO_ONLY, MediaFormat.MPEG_4, "144p HDR", 30),
            };

            for (final ItagItem item : extendedItems) {
                if (!existingIds.contains(item.id)) {
                    updatedList.add(item);
                }
            }

            final ItagItem[] finalArray = updatedList.toArray(new ItagItem[0]);

            // Attempt setting via standard reflection first (works on Android ART)
            try {
                itagListField.set(null, finalArray);
            } catch (final Throwable reflectionError) {
                // On standard JVM (e.g. desktop/CI tests with Java 17/21), fall back to Unsafe
                try {
                    final Field unsafeField = Class.forName("sun.misc.Unsafe")
                            .getDeclaredField("theUnsafe");
                    unsafeField.setAccessible(true);
                    final Object unsafe = unsafeField.get(null);
                    final Method staticFieldBase = unsafe.getClass()
                            .getMethod("staticFieldBase", Field.class);
                    final Method staticFieldOffset = unsafe.getClass()
                            .getMethod("staticFieldOffset", Field.class);
                    final Method putObject = unsafe.getClass()
                            .getMethod("putObject", Object.class, long.class, Object.class);

                    final Object base = staticFieldBase.invoke(unsafe, itagListField);
                    final long offset = (long) staticFieldOffset.invoke(unsafe, itagListField);
                    putObject.invoke(unsafe, base, offset, finalArray);
                } catch (final Throwable unsafeError) {
                    logE("Unsafe fallback failed to set ITAG_LIST", unsafeError);
                }
            }

            initialized = true;
            logD("Extended itags initialized successfully: "
                    + (finalArray.length - originalList.length) + " new itags registered.");
        } catch (final Exception e) {
            logE("Failed to initialize extended itags", e);
        }
    }

    private static void logD(final String msg) {
        try {
            Log.d(TAG, msg);
        } catch (final Throwable ignored) {
            System.out.println(TAG + ": " + msg);
        }
    }

    private static void logE(final String msg, final Throwable t) {
        try {
            Log.e(TAG, msg, t);
        } catch (final Throwable ignored) {
            System.err.println(TAG + ": " + msg);
        }
    }
}
