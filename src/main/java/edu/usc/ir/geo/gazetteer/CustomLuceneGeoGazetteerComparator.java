/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
		 * All FeatureClass present in GeoName data set.<br/>
		 *  A country, state, region <br/>
		 *  H stream, lake <br/>
		 *  L parks,area <br/>
		 *  P city, village <br/>
		 *  R road, railroad <br/>
		 *  S spot, building, farm  <br/>
		 *  T mountain,hill,rock <br/> 
		 *  U undersea  <br/>
		 *  V forest,heath <br/>
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
	 * Currently sort order is defined only for most used feature class A and P <br/>
	 * http://www.geonames.org/export/codes.html
	 * <br/><br/>
	 * Class - A<br/>
	 * ADM1 - first-order administrative division<br/>
	 * ADM1H - historical first-order administrative division<br/>
	 * ADM2 - second-order administrative division<br/>
	 * ADM2H - historical second-order administrative division<br/>
	 * ADM3 - third-order administrative division<br/>
	 * ADM3H - historical third-order administrative division<br/>
	 * ADM4 - fourth-order administrative division<br/>
	 * ADM4H - historical fourth-order administrative division<br/>
	 * ADM5 - fifth-order administrative division<br/>
	 * ADMD - administrative division<br/>
	 * ADMDH - historical administrative division <br/>
	 * LTER - leased area<br/>
	 * PCL - political entity<br/>
	 * PCLD - dependent political entity<br/>
	 * PCLF - freely associated state<br/>
	 * PCLH - historical political entity<br/>
	 * PCLI - independent political entity<br/>
	 * PCLIX - section of independent political entity<br/>
	 * PCLS - semi-independent political entity<br/>
	 * PRSH - parish<br/>
	 * TERR - territory<br/>
	 * ZN - zone<br/>
	 * ZNB - buffer zone<br/>
	 * <br/>
	 * Class - P<br/>
	 * PPL - populated place<br/>
	 * PPLA - seat of a first-order administrative division<br/>
	 * PPLA2 - seat of a second-order administrative division<br/>
	 * PPLA3 - seat of a third-order administrative division<br/>
	 * PPLA4 - seat of a fourth-order administrative division<br/>
	 * PPLC - capital of a political entity<br/>
	 * PPLCH - historical capital of a political entity<br/>
	 * PPLF - farm village<br/>
	 * PPLG - seat of government of a political entity<br/>
	 * PPLH - historical populated place<br/>
	 * PPLL - populated locality<br/>
	 * PPLQ - abandoned populated place<br/>
	 * PPLR - religious populated place<br/>
	 * PPLS - populated places<br/>
	 * PPLW - destroyed populated place<br/>
	 * PPLX - section of populated place<br/>
	 * STLMT - israeli settlement
	 */
	static class FeatureCodeComparator extends FieldComparatorSource {

		public static enum FeatureCode {
			// A country, state, region
			TERR, PCLI, PCLD, PCLIX, PCLF, PCL, PCLS, ADM1, ADMD, ADM2, LTER, ADM3, ADM4, ADM5, PRSH, ZN, ZNB, PCLH, ADM1H, ADM2H, ADM3H, ADM4H, ADMDH,
		
			// P city, village
			PPLC, PPL, PPLA, PPLA2, PPLA3, PPLA4, STLMT, PPLS, PPLG, PPLF, PPLL, PPLR, PPLX, PPLW, PPLCH, PPLH, PPLQ,

			// S spot, building, farm
			ADMF, AGRF, AIRB, AIRF, AIRH, AIRP, AIRQ, AMTH, ANS, AQC, ARCH, ASTR, ASYL, ATHF, ATM, BANK, BCN, BDG, BDGQ, BLDG, BLDO, BP, BRKS, BRKW, BSTN, BTYD, BUR, BUSTN, BUSTP, CARN, CAVE, CH, CMP, CMPL, CMPLA, CMPMN, CMPO, CMPQ, CMPRF, CMTY, COMC, CRRL, CSNO, CSTL, CSTM, CTHSE, CTRA, CTRCM, CTRF, CTRM, CTRR, CTRS, CVNT, DAM, DAMQ, DAMSB, DARY, DCKD, DCKY, DIKE, DIP, DPOF, EST, ESTO, ESTR, ESTSG, ESTT, ESTX, FCL, FNDY, FRM, FRMQ, FRMS, FRMT, FT, FY, GATE, GDN, GHAT, GHSE, GOSP, GOVL, GRVE, HERM, HLT, HMSD, HSE, HSEC, HSP, HSPC, HSPD, HSPL, HSTS, HTL, HUT, HUTS, INSM, ITTR, JTY, LDNG, LEPC, LIBR, LNDF, LOCK, LTHSE, MALL, MAR, MFG, MFGB, MFGC, MFGCU, MFGLM, MFGM, MFGPH, MFGQ, MFGSG, MKT, ML, MLM, MLO, MLSG, MLSGQ, MLSW, MLWND, MLWTR, MN, MNAU, MNC, MNCR, MNCU, MNFE, MNMT, MNN, MNQ, MNQR, MOLE, MSQE, MSSN, MSSNQ, MSTY, MTRO, MUS, NOV, NSY, OBPT, OBS, OBSR, OILJ, OILQ, OILR, OILT, OILW, OPRA, PAL, PGDA, PIER, PKLT, PMPO, PMPW, PO, PP, PPQ, PRKGT, PRKHQ, PRN, PRNJ, PRNQ, PS, PSH, PSTB, PSTC, PSTP, PYR, PYRS, QUAY, RDCR, RECG, RECR, REST, RET, RHSE, RKRY, RLG, RLGR, RNCH, RSD, RSGNL, RSRT, RSTN, RSTNQ, RSTP, RSTPQ, RUIN, SCH, SCHA, SCHC, SCHL, SCHM, SCHN, SCHT, SECP, SHPF, SHRN, SHSE, SLCE, SNTR, SPA, SPLY, SQR, STBL, STDM, STNB, STNC, STNE, STNF, STNI, STNM, STNR, STNS, STNW, STPS, SWT, THTR, TMB, TMPL, TNKD, TOWR, TRANT, TRIG, TRMO, TWO, UNIP, UNIV, USGE, VETF, WALL, WALLA, WEIR, WHRF, WRCK, WTRW, ZNF, ZOO,

			// T mountain,hill,rock
			ASPH, ATOL, BAR, BCH, BCHS, BDLD, BLDR, BLHL, BLOW, BNCH, BUTE, CAPE, CFT, CLDA, CLF, CNYN, CONE, CRDR, CRQ, CRQS, CRTR, CUET, DLTA, DPR, DSRT, DUNE, DVD, ERG, FAN, FORD, FSR, GAP, GRGE, HDLD, HLL, HLLS, HMCK, HMDA, INTF, ISL, ISLET, ISLF, ISLM, ISLS, ISLT, ISLX, ISTH, KRST, LAVA, LEV, MESA, MND, MRN, MT, MTS, NKM, NTK, NTKS, PAN, PANS, PASS, PEN, PENX, PK, PKS, PLAT, PLATX, PLDR, PLN, PLNX, PROM, PT, PTS, RDGB, RDGE, REG, RK, RKFL, RKS, SAND, SBED, SCRP, SDL, SHOR, SINK, SLID, SLP, SPIT, SPUR, TAL, TRGD, TRR, UPLD, VAL, VALG, VALS, VALX, VLC,

			// L parks,area
			AGRC, AMUS, AREA, BSND, BSNP, BTL, CLG, CMN, CNS, COLF, CONT, CST, CTRB, DEVH, FLD, FLDI, GASF, GRAZ, GVL, INDS, LAND, LCTY, MILB, MNA, MVA, NVB, OAS, OILF, PEAT, PRK, PRT, QCKS, RES, RESA, RESF, RESH, RESN, RESP, RESV, RESW, RGN, RGNE, RGNH, RGNL, RNGA, SALT, SNOW, TRB,

			// H stream, lake, ...
			AIRS, ANCH, BAY, BAYS, BGHT, BNK, BNKR, BNKX, BOG, CAPG, CHN, CHNL, CHNM, CHNN, CNFL, CNL, CNLA, CNLB, CNLD, CNLI, CNLN, CNLQ, CNLSB, CNLX, COVE, CRKT, CRNT, CUTF, DCK, DCKB, DOMG, DPRG, DTCH, DTCHD, DTCHI, DTCHM, ESTY, FISH, FJD, FJDS, FLLS, FLLSX, FLTM, FLTT, GLCR, GULF, GYSR, HBR, HBRX, INLT, INLTQ, LBED, LGN, LGNS, LGNX, LK, LKC, LKI, LKN, LKNI, LKO, LKOI, LKS, LKSB, LKSC, LKSI, LKSN, LKSNI, LKX, MFGN, MGV, MOOR, MRSH, MRSHN, NRWS, OCN, OVF, PND, PNDI, PNDN, PNDNI, PNDS, PNDSF, PNDSI, PNDSN, POOL, POOLI, RCH, RDGG, RDST, RF, RFC, RFX, RPDS, RSV, RSVI, RSVT, RVN, SBKH, SD, SEA, SHOL, SILL, SPNG, SPNS, SPNT, STM, STMA, STMB, STMC, STMD, STMH, STMI, STMIX, STMM, STMQ, STMS, STMSB, STMX, STRT, SWMP, SYSI, TNLC, WAD, WADB, WADJ, WADM, WADS, WADX, WHRL, WLL, WLLQ, WLLS, WTLD, WTLDI, WTRC, WTRH,

			// R road, railroad
			CSWY, OILP, PRMN, PTGE, RD, RDA, RDB, RDCUT, RDJCT, RJCT, RR, RRQ, RTE, RYD, ST, STKR, TNL, TNLN, TNLRD, TNLRR, TNLS, TRL,

			// V forest,heath
			BUSH, CULT, FRST, FRSTF, GRSLD, GRVC, GRVO, GRVP, GRVPN, HTH, MDW, OCH, SCRB, TREE, TUND, VIN, VINS, ll,
		
			// U undersea
			APNU, ARCU, ARRU, BDLU, BKSU, BNKU, BSNU, CDAU, CNSU, CNYU, CRSU, DEPU, EDGU, ESCU, FANU, FLTU, FRZU, FURU, GAPU, GLYU, HLLU, HLSU, HOLU, KNLU, KNSU, LDGU, LEVU, MESU, MNDU, MOTU, MTU, PKSU, PKU, PLNU, PLTU, PNLU, PRVU, RDGU, RDSU, RFSU, RFU, RISU, SCNU, SCSU, SDLU, SHFU, SHLU, SHSU, SHVU, SILU, SLPU, SMSU, SMU, SPRU, TERU, TMSU, TMTU, TNGU, TRGU, TRNU, VALU, VLSU;
		
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
