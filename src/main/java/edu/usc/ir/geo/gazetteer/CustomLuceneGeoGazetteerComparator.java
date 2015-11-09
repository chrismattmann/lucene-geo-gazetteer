package edu.usc.ir.geo.gazetteer;

import java.io.IOException;
import java.util.logging.Logger;

import org.apache.commons.lang3.EnumUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldComparatorSource;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;

/**
 * Custom FieldComparatorSource for indexed GeoNames data set
 */
public class CustomLuceneGeoGazetteerComparator {
	private static final Logger LOG = Logger.getLogger(CustomLuceneGeoGazetteerComparator.class.getName());

	/**
	 * Defines custom sort order for "feature class" field. <br/>
	 * Current proposed sort order is -> A P S T L H R V U <br/>
	 * 
	 */
	static class FeatureClassComparator extends FieldComparatorSource {

		/**
		 * All FeatureClass present in GeoName data set
		 */
		public static enum FeatureClass {
			A, P, S, T, L, H, R, V, U;

			public static boolean exists(String val) {
				return EnumUtils.isValidEnum(FeatureClass.class, val);
			}
		}

		@Override
		public FieldComparator<BytesRef> newComparator(final String fieldName, final int numHits, final int sortPos,
				boolean reversed) throws IOException {
			// This Comparator is only meant for "feature class" field.
			if (!GeoNameResolver.FIELD_NAME_FEATURE_CLASS.equals(fieldName)) {
				throw new RuntimeException(
						"FeatureClassComparatorSource is not supposed to run on any other field apart from "
								+ GeoNameResolver.FIELD_NAME_FEATURE_CLASS);
			}

			return new FieldComparator.TermValComparator(numHits, fieldName, reversed) {

				public int compareValues(BytesRef val1, BytesRef val2) {
					String val1String = StringUtils.trim(new String(val1.bytes));
					String val2String = StringUtils.trim(new String(val2.bytes));

					int res;
					try {
						res = FeatureClass.valueOf(val1String).compareTo(FeatureClass.valueOf(val2String));
					} catch (IllegalArgumentException e) {
						// If feature code is not present in Enum return 0.
						// This is to safeguard from future changes in GeoNames data set
						LOG.warning(e.getMessage());
						
						// give higher rank to values present in enum
						//if enum is present it's treated as smaller than other
						res = !FeatureClass.exists(val1String) ? !FeatureClass.exists(val2String) ? 0 : 1000 : -1000;
					}

					return res;

				}
			};
		}
	}

	/**
	 * Defines custom sort order for "feature code" field. <br/>
	 * Currently sort order is defined only for most used feature class A and P
	 * <br/>
	 */
	static class FeatureCodeComparator extends FieldComparatorSource {

		public static enum FeatureCode {
			// A
			TERR, PCLI, PCLD, PCLIX, PCLF, PCL, PCLS, ADM1, ADM2, LTER, ADM3, ADM4, ADM5, ADMD, PRSH, ZN, ZNB, PCLH, ADM1H, ADM2H, ADM3H, ADM4H, ADMDH,
			// P
			PPLC, PPL, PPLA, PPLA2, PPLA3, PPLA4, STLMT, PPLS, PPLG, PPLF, PPLL, PPLR, PPLX, PPLW, PPLCH, PPLH, PPLQ;

			public static boolean exists(String val) {
				return EnumUtils.isValidEnum(FeatureCode.class, val);
			}
		}

		@Override
		public FieldComparator<BytesRef> newComparator(final String fieldName, final int numHits, final int sortPos,
				boolean reversed) throws IOException {

			// This Comparator is only meant for "feature code" field.
			if (!GeoNameResolver.FIELD_NAME_FEATURE_CODE.equals(fieldName)) {
				throw new RuntimeException(
						"FeatureClassComparatorSource is not supposed to run on any other field apart from "
								+ GeoNameResolver.FIELD_NAME_FEATURE_CODE);
			}

			return new FieldComparator.TermValComparator(numHits, fieldName, reversed) {

				public int compareValues(BytesRef val1, BytesRef val2) {
					String val1String = StringUtils.trim(new String(val1.bytes));
					String val2String = StringUtils.trim(new String(val2.bytes));

					int res;
					try {
						res = FeatureCode.valueOf(val1String).compareTo(FeatureCode.valueOf(val2String));

					} catch (IllegalArgumentException e) {
						// If feature code is not present in Enum return 0.
						// This is to safeguard from future changes in GeoNamesdata set
						// LOG.warning(e.getMessage()); //Uncomment when implemented all codes

						// give higher rank to values present in enum
						//if enum is present it's treated as smaller than other
						res = !FeatureCode.exists(val1String) ? !FeatureCode.exists(val2String) ? 0 : 1000 : -1000;
					}

					return res;

				}
			};
		}
	}

	/**
	 * @return custom sort order for "feature code" field.
	 */
	public static SortField getFeatureClassSortField() {
		return new SortField(GeoNameResolver.FIELD_NAME_FEATURE_CLASS,
				new CustomLuceneGeoGazetteerComparator.FeatureClassComparator());
	}

	/**
	 * @return custom sort order for "feature code" field.
	 */
	public static SortField getFeatureCodeSortField() {
		return new SortField(GeoNameResolver.FIELD_NAME_FEATURE_CODE, new CustomLuceneGeoGazetteerComparator.FeatureCodeComparator());
	}

}
