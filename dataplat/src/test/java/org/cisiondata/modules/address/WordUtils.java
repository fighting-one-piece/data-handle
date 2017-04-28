package org.cisiondata.modules.address;

import java.util.List;

import org.apdplat.word.WordSegmenter;
import org.apdplat.word.segmentation.SegmentationAlgorithm;
import org.apdplat.word.segmentation.Word;
import org.apdplat.word.util.WordConfTools;

public class WordUtils {
	
	/**
	  	SegmentationAlgorithm的可选类型为：	 
		正向最大匹配算法：MaximumMatching
		逆向最大匹配算法：ReverseMaximumMatching
		正向最小匹配算法：MinimumMatching
		逆向最小匹配算法：ReverseMinimumMatching
		双向最大匹配算法：BidirectionalMaximumMatching
		双向最小匹配算法：BidirectionalMinimumMatching
		双向最大最小匹配算法：BidirectionalMaximumMinimumMatching
		全切分算法：FullSegmentation
		最少词数算法：MinimalWordCount
		最大Ngram分值算法：MaxNgramScore
	 */
	public static void segmenter(String input, SegmentationAlgorithm segmentationAlgorithm) {
		List<Word> words = WordSegmenter.seg(input, segmentationAlgorithm);
		for (Word word : words) {
			System.out.print(word + "  ");
		}
		System.out.println();
		System.out.println("###########");
	}

	public static void main(String[] args) {
//		segmenter("湖北省武汉市武昌区珞珈山路16号武汉大学", SegmentationAlgorithm.MaximumMatching);
//		segmenter("湖北省武汉市武昌区珞珈山路16号武汉大学", SegmentationAlgorithm.ReverseMaximumMatching);
//		segmenter("湖北省武汉市武昌区珞珈山路16号武汉大学", SegmentationAlgorithm.BidirectionalMaximumMatching);
//		segmenter("湖北省武汉市武昌区珞珈山路16号武汉大学", SegmentationAlgorithm.BidirectionalMaximumMinimumMatching);
//		segmenter("湖北省武汉市武昌区珞珈山路16号武汉大学", SegmentationAlgorithm.MinimalWordCount);
//		segmenter("湖北省武汉市武昌区珞珈山路16号武汉大学", SegmentationAlgorithm.MaxNgramScore);
//		segmenter("湖北省武汉市武昌区珞珈山路16号武汉大学", SegmentationAlgorithm.FullSegmentation);
		WordConfTools.set("dic.path", "classpath:word/customdic.txt");
		segmenter("湖北省武汉市武昌区珞珈山路16号武汉大学", SegmentationAlgorithm.MaximumMatching);
		segmenter("湖北省武汉武昌区珞珈山路16号武汉大学", SegmentationAlgorithm.MaximumMatching);
		segmenter("湖北武汉市武昌区珞珈山路16号武汉大学", SegmentationAlgorithm.MaximumMatching);
		segmenter("湖北武汉武昌区珞珈山路16号武汉大学", SegmentationAlgorithm.MaximumMatching);
		
	}
	
}
