package org.platform.modules.word;

import java.util.List;

import org.apdplat.word.WordSegmenter;
import org.apdplat.word.segmentation.SegmentationAlgorithm;
import org.apdplat.word.segmentation.Word;

public class WordUtils {

	private static void segmenter(String input, SegmentationAlgorithm segmentationAlgorithm) {
		List<Word> words = WordSegmenter.seg(input, segmentationAlgorithm);
		for (int i = 0, len = words.size(); i < len; i++) {
			String word = words.get(i).getText();
			System.out.println(word);
			
		}
		System.out.println();
		System.out.println("###########");
	}
	
	public static void main(String[] args) {
//		segmenter("湖北省武汉市武昌区珞珈山路16号武汉大学", SegmentationAlgorithm.MaximumMatching);
//		segmenter("湖北省武汉武昌区珞珈山路16号武汉大学", SegmentationAlgorithm.MaximumMatching);
//		segmenter("浙江省诸暨市暨阳街道永福新村36幢二单元302室", SegmentationAlgorithm.MaximumMatching);
//		segmenter("吉林省延边朝鲜族自治州市珲春市春化镇邮局书到了电联", SegmentationAlgorithm.MaximumMatching);
//		segmenter("云南省楚雄彝族自治州楚雄市牟定县凤屯镇电联未妥投保留七天", SegmentationAlgorithm.MaximumMatching);
//		segmenter("湖南省衡阳市水口山镇邮局（顾客出来接）\u001d", SegmentationAlgorithm.MaximumMatching);
		segmenter("云南省昆明市盘龙区新东方学校(尚义街)", SegmentationAlgorithm.MaximumMatching);
	}
	
}
