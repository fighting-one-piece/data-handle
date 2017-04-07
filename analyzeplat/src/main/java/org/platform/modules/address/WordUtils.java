package org.platform.modules.address;

import java.util.List;

import org.apdplat.word.WordSegmenter;
import org.apdplat.word.segmentation.SegmentationAlgorithm;
import org.apdplat.word.segmentation.Word;

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
	public void segmenter() {
		
	}

	public static void main(String[] args) {
		List<Word> words = WordSegmenter.seg("应用级产品开发平台", SegmentationAlgorithm.MaximumMatching);
		for (Word word : words) {
			System.out.println(word);
		}
	}
	
}
