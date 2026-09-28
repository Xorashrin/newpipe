package org.schabi.newpipe.util;

import org.junit.Test;
import org.schabi.newpipe.extractor.MediaFormat;
import org.schabi.newpipe.extractor.services.youtube.ItagItem;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ItagHelperTest {

    @Test
    public void testExtendedItagsRegistration() throws Exception {
        ItagHelper.initExtendedItags();

        // 8K AV1 itag
        final ItagItem item571 = ItagItem.getItag(571);
        assertNotNull(item571);
        assertEquals(571, item571.id);
        assertEquals("4320p60", item571.getResolutionString());

        // 8K AV1 HDR itag
        final ItagItem item702 = ItagItem.getItag(702);
        assertNotNull(item702);
        assertEquals(702, item702.id);
        assertEquals("4320p60 HDR", item702.getResolutionString());

        // VP9.2 HDR itag
        final ItagItem item337 = ItagItem.getItag(337);
        assertNotNull(item337);
        assertEquals(337, item337.id);
        assertEquals(MediaFormat.WEBM, item337.getMediaFormat());
    }
}
