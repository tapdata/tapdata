package io.tapdata.pdk.apis.partition.splitter;

import io.tapdata.pdk.apis.entity.QueryOperator;
import io.tapdata.pdk.apis.partition.FieldMinMaxValue;
import io.tapdata.pdk.apis.partition.TapPartitionFilter;

import java.util.ArrayList;
import java.util.List;

import static io.tapdata.entity.simplify.TapSimplify.list;

/**
 * @author aplomb
 */
public class StringSplitter implements TypeSplitter<String> {
	//https://www.ssec.wisc.edu/~tomw/java/unicode.html
	static String unicodeChart = "0x0000-0x007F\t0-127\tBasic Latin\n" +
			"0x0080-0x00FF\t128-255\tLatin-1 Supplement\n" +
			"0x0100-0x017F\t256-383\tLatin Extended-A\n" +
			"0x0180-0x024F\t384-591\tLatin Extended-B\n" +
			"0x0250-0x02AF\t592-687\tIPA Extensions\n" +
			"0x02B0-0x02FF\t688-767\tSpacing Modifier Letters\n" +
			"0x0300-0x036F\t768-879\tCombining Diacritical Marks\n" +
			"0x0370-0x03FF\t880-1023\tGreek\n" +
			"0x0400-0x04FF\t1024-1279\tCyrillic\n" +
			"0x0530-0x058F\t1328-1423\tArmenian\n" +
			"0x0590-0x05FF\t1424-1535\tHebrew\n" +
			"0x0600-0x06FF\t1536-1791\tArabic\n" +
			"0x0700-0x074F\t1792-1871\tSyriac\n" +
			"0x0780-0x07BF\t1920-1983\tThaana\n" +
			"0x0900-0x097F\t2304-2431\tDevanagari\n" +
			"0x0980-0x09FF\t2432-2559\tBengali\n" +
			"0x0A00-0x0A7F\t2560-2687\tGurmukhi\n" +
			"0x0A80-0x0AFF\t2688-2815\tGujarati\n" +
			"0x0B00-0x0B7F\t2816-2943\tOriya\n" +
			"0x0B80-0x0BFF\t2944-3071\tTamil\n" +
			"0x0C00-0x0C7F\t3072-3199\tTelugu\n" +
			"0x0C80-0x0CFF\t3200-3327\tKannada\n" +
			"0x0D00-0x0D7F\t3328-3455\tMalayalam\n" +
			"0x0D80-0x0DFF\t3456-3583\tSinhala\n" +
			"0x0E00-0x0E7F\t3584-3711\tThai\n" +
			"0x0E80-0x0EFF\t3712-3839\tLao\n" +
			"0x0F00-0x0FFF\t3840-4095\tTibetan\n" +
			"0x1000-0x109F\t4096-4255\tMyanmar\n" +
			"0x10A0-0x10FF\t4256-4351\tGeorgian\n" +
			"0x1100-0x11FF\t4352-4607\tHangul Jamo\n" +
			"0x1200-0x137F\t4608-4991\tEthiopic\n" +
			"0x13A0-0x13FF\t5024-5119\tCherokee\n" +
			"0x1400-0x167F\t5120-5759\tUnified Canadian Aboriginal Syllabics\n" +
			"0x1680-0x169F\t5760-5791\tOgham\n" +
			"0x16A0-0x16FF\t5792-5887\tRunic\n" +
			"0x1780-0x17FF\t6016-6143\tKhmer\n" +
			"0x1800-0x18AF\t6144-6319\tMongolian\n" +
			"0x1E00-0x1EFF\t7680-7935\tLatin Extended Additional\n" +
			"0x1F00-0x1FFF\t7936-8191\tGreek Extended\n" +
			"0x2000-0x206F\t8192-8303\tGeneral Punctuation\n" +
			"0x2070-0x209F\t8304-8351\tSuperscripts and Subscripts\n" +
			"0x20A0-0x20CF\t8352-8399\tCurrency Symbols\n" +
			"0x20D0-0x20FF\t8400-8447\tCombining Marks for Symbols\n" +
			"0x2100-0x214F\t8448-8527\tLetterlike Symbols\n" +
			"0x2150-0x218F\t8528-8591\tNumber Forms\n" +
			"0x2190-0x21FF\t8592-8703\tArrows\n" +
			"0x2200-0x22FF\t8704-8959\tMathematical Operators\n" +
			"0x2300-0x23FF\t8960-9215\tMiscellaneous Technical\n" +
			"0x2400-0x243F\t9216-9279\tControl Pictures\n" +
			"0x2440-0x245F\t9280-9311\tOptical Character Recognition\n" +
			"0x2460-0x24FF\t9312-9471\tEnclosed Alphanumerics\n" +
			"0x2500-0x257F\t9472-9599\tBox Drawing\n" +
			"0x2580-0x259F\t9600-9631\tBlock Elements\n" +
			"0x25A0-0x25FF\t9632-9727\tGeometric Shapes\n" +
			"0x2600-0x26FF\t9728-9983\tMiscellaneous Symbols\n" +
			"0x2700-0x27BF\t9984-10175\tDingbats\n" +
			"0x2800-0x28FF\t10240-10495\tBraille Patterns\n" +
			"0x2E80-0x2EFF\t11904-12031\tCJK Radicals Supplement\n" +
			"0x2F00-0x2FDF\t12032-12255\tKangxi Radicals\n" +
			"0x2FF0-0x2FFF\t12272-12287\tIdeographic Description Characters\n" +
			"0x3000-0x303F\t12288-12351\tCJK Symbols and Punctuation\n" +
			"0x3040-0x309F\t12352-12447\tHiragana\n" +
			"0x30A0-0x30FF\t12448-12543\tKatakana\n" +
			"0x3100-0x312F\t12544-12591\tBopomofo\n" +
			"0x3130-0x318F\t12592-12687\tHangul Compatibility Jamo\n" +
			"0x3190-0x319F\t12688-12703\tKanbun\n" +
			"0x31A0-0x31BF\t12704-12735\tBopomofo Extended\n" +
			"0x3200-0x32FF\t12800-13055\tEnclosed CJK Letters and Months\n" +
			"0x3300-0x33FF\t13056-13311\tCJK Compatibility\n" +
			"0x3400-0x4DB5\t13312-19893\tCJK Unified Ideographs Extension A\n" +
			"0x4E00-0x9FFF\t19968-40959\tCJK Unified Ideographs\n" +
			"0xA000-0xA48F\t40960-42127\tYi Syllables\n" +
			"0xA490-0xA4CF\t42128-42191\tYi Radicals\n" +
			"0xAC00-0xD7A3\t44032-55203\tHangul Syllables\n" +
			"0xD800-0xDB7F\t55296-56191\tHigh Surrogates\n" +
			"0xDB80-0xDBFF\t56192-56319\tHigh Private Use Surrogates\n" +
			"0xDC00-0xDFFF\t56320-57343\tLow Surrogates\n" +
			"0xE000-0xF8FF\t57344-63743\tPrivate Use\n" +
			"0xF900-0xFAFF\t63744-64255\tCJK Compatibility Ideographs\n" +
			"0xFB00-0xFB4F\t64256-64335\tAlphabetic Presentation Forms\n" +
			"0xFB50-0xFDFF\t64336-65023\tArabic Presentation Forms-A\n" +
			"0xFE20-0xFE2F\t65056-65071\tCombining Half Marks\n" +
			"0xFE30-0xFE4F\t65072-65103\tCJK Compatibility Forms\n" +
			"0xFE50-0xFE6F\t65104-65135\tSmall Form Variants\n" +
			"0xFE70-0xFEFE\t65136-65278\tArabic Presentation Forms-B\n" +
			"0xFEFF-0xFEFF\t65279-65279\tSpecials\n" +
			"0xFF00-0xFFEF\t65280-65519\tHalfwidth and Fullwidth Forms\n" +
			"0xFFF0-0xFFFD\t65520-65533\tSpecials";
//	static {
//
//	}
	public static StringSplitter INSTANCE = new StringSplitter();

	public boolean caseInsensitive() {
		return false;
	}

	@Override
	public List<TapPartitionFilter> split(TapPartitionFilter boundaryPartitionFilter, FieldMinMaxValue fieldMinMaxValue, int maxSplitPieces) {
		String min = (String) fieldMinMaxValue.getMin();
		String max = (String) fieldMinMaxValue.getMax();
		if(caseInsensitive()) {
			min = min.toUpperCase();
			max = max.toUpperCase();
		}
		char[] minChars = min.toCharArray();
		char[] maxChars = max.toCharArray();
		int maxLength = Math.max(minChars.length, maxChars.length);
		int pos = 0;
		char minChar = 32, maxChar = 32;
		boolean different = false;
		for(int i = 0; i < maxLength; i++) {
			pos = i;
			if(i == minChars.length) {
				maxChar = maxChars[i];
				minChar = 32; //space
				different = true;
				break;
			}
			if(i == maxChars.length) {
				maxChar = 32; //space
				minChar = minChars[i];
				different = true;
				break;
			}
			if(minChars[i] != maxChars[i]) {
				minChar = minChars[i];
				maxChar = maxChars[i];
				different = true;
				break;
			}
		}
		if(different) {
			String minStr = min.substring(0, pos) + minChar;
			String maxStr = max.substring(0, pos) + maxChar;
			int value = maxChar - minChar;
			int pieceSize = value / maxSplitPieces;
			if(pieceSize == 0) {
				pieceSize = 1;
				maxSplitPieces = value / pieceSize;
			}
			List<TapPartitionFilter> partitionFilters = new ArrayList<>();
			for(int i = 0; i < maxSplitPieces; i++) {
				if(i == 0) {
					partitionFilters.add(TapPartitionFilter.create().resetMatch(boundaryPartitionFilter.getMatch())
							.leftBoundary(boundaryPartitionFilter.getLeftBoundary())
							.rightBoundary(QueryOperator.lt(fieldMinMaxValue.getFieldName(), min.substring(0, pos) + (char)(minChar + pieceSize))));
				} else if(i == maxSplitPieces - 1) {
					partitionFilters.add(TapPartitionFilter.create().resetMatch(boundaryPartitionFilter.getMatch())
							.leftBoundary(QueryOperator.gte(fieldMinMaxValue.getFieldName(), min.substring(0, pos) + (char)(minChar + pieceSize * i)))
							.rightBoundary(boundaryPartitionFilter.getRightBoundary()));
				} else {
					partitionFilters.add(TapPartitionFilter.create().resetMatch(boundaryPartitionFilter.getMatch())
							.leftBoundary(QueryOperator.gte(fieldMinMaxValue.getFieldName(), min.substring(0, pos) + (char)(minChar + pieceSize * i)))
							.rightBoundary(QueryOperator.lt(fieldMinMaxValue.getFieldName(), min.substring(0, pos) + (char)(minChar + pieceSize * (i + 1))))
					);
				}
			}
			if(maxSplitPieces == 1) {
				partitionFilters.add(TapPartitionFilter.create().resetMatch(boundaryPartitionFilter.getMatch())
						.leftBoundary(QueryOperator.gte(fieldMinMaxValue.getFieldName(), min.substring(0, pos) + (char)(minChar + pieceSize)))
						.rightBoundary(boundaryPartitionFilter.getRightBoundary()));
			}
			return partitionFilters;
		} else {
			return new ArrayList<>(TapPartitionFilter.filtersWhenMinMaxEquals(boundaryPartitionFilter, fieldMinMaxValue, min));
		}
	}

	@Override
	public int compare(String o1, String o2) {
		return o1.compareTo(o2);
	}
}
