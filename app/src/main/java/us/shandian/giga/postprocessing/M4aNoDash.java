package us.shandian.giga.postprocessing;

import org.schabi.newpipe.streams.Mp4DashReader;
import org.schabi.newpipe.streams.Mp4FromDashWriter;
import org.schabi.newpipe.streams.io.SharpStream;

import java.io.IOException;

class M4aNoDash extends Postprocessing {

    M4aNoDash() {
        super(false, true, ALGORITHM_M4A_NO_DASH);
    }

    @Override
    boolean test(SharpStream... sources) throws IOException {
        // check if the mp4 file is DASH (youtube)
        try {
            Mp4DashReader reader = new Mp4DashReader(sources[0]);
            reader.parse();

            int[] brands = reader.getBrands();
            if (brands != null && brands.length > 0) {
                switch (brands[0]) {
                    case 0x64617368:// DASH
                    case 0x69736F35:// ISO5
                        return true;
                    default:
                        return false;
                }
            }
            return false;
        } catch (Exception e) {
            // Not a DASH MP4 container (e.g. standard progressive video/audio), skip demuxing
            return false;
        }
    }

    @Override
    int process(SharpStream out, SharpStream... sources) throws IOException {
        Mp4FromDashWriter muxer = new Mp4FromDashWriter(sources[0]);
        muxer.setMainBrand(0x4D344120);// binary string "M4A "
        muxer.parseSources();
        muxer.selectTracks(0);
        muxer.build(out);

        return OK_RESULT;
    }
}
