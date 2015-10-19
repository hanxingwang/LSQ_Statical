package cn.tju.edu.dataUtil;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WelldesignUtil {
	private static int totalCount = 0;
	private static int wellCount = 0;
	private static int notWellCount = 0;
	private static int unionCount = 0;
	private static int subQueryCount = 0;
	
	private static int unionNormalFormCount = 0;
	private static int unUnionNormalFormCount = 0;
	private static int canNotChangeToUnionFreeCount = 0;
	private static int unionWithNotFirstBraceCount = 0;
	private static int unionNormalFormWithFilterAndOptionalCount = 0;

	public static boolean isWelldesign(String queryString, boolean isSubQuery) {
		String whereClause = null;
		String upperString = queryString.toUpperCase();

//		if(upperString.contains("GRAPH"))
//			return false;
		if(!isSubQuery)
			totalCount++;
		
		int whereClauseBegin, whereClauseEnd;

		whereClauseBegin = upperString.indexOf('{', upperString.indexOf("WHERE"));

		BufferedWriter wellDesignedBufferedWriter = null;
		BufferedWriter unWellDesignedBufferedWriter = null;
		BufferedWriter unionBufferedWriter = null;
		BufferedWriter subQueryBufferedWriter = null;
		
		BufferedWriter unionNormalFormBufferedWriter = null;
		BufferedWriter unUnionNormalFormBufferedWriter = null;
		BufferedWriter canNotChangeToUnionFreeBufferedWriter = null;
		BufferedWriter unionWithNotFirstBraceBufferedWriter = null;
		BufferedWriter unionNormalFormWithFilterAndOptionalBufferedWriter = null;

		try {
			FileWriter wellDesignedFileWriter = new FileWriter("/home/hanxingwang/Data/SearchResult/WellDesigned",
					true);
			FileWriter unWellDesignedfileWriter = new FileWriter("/home/hanxingwang/Data/SearchResult/UnWellDesigned",
					true);
			FileWriter unionFileWriter = new FileWriter("/home/hanxingwang/Data/SearchResult/NotUnionFree", true);
			FileWriter subQueryWriter = new FileWriter("/home/hanxingwang/Data/SearchResult/subQuery", true);
			
			FileWriter unionNormalFormWrite = new FileWriter("/home/hanxingwang/Data/SearchResult/unionNormalForm", true);
			FileWriter unUnionNormalFormWrite = new FileWriter("/home/hanxingwang/Data/SearchResult/unUnionNormalForm1", true);
			FileWriter canNotChangeToUnionFreeWrite = new FileWriter("/home/hanxingwang/Data/SearchResult/canNotChangeToUnionNormalForm", true);
			FileWriter unionWithNotFirstBraceWrite = new FileWriter("/home/hanxingwang/Data/SearchResult/unionWithNoFirstBrace", true);
			FileWriter unionNormalFormWithFilterAndOptionalWrite = new FileWriter("/home/hanxingwang/Data/SearchResult/unionNormalFormWithFilterAndOptional", true);

			wellDesignedBufferedWriter = new BufferedWriter(wellDesignedFileWriter);
			unWellDesignedBufferedWriter = new BufferedWriter(unWellDesignedfileWriter);
			unionBufferedWriter = new BufferedWriter(unionFileWriter);
			subQueryBufferedWriter = new BufferedWriter(subQueryWriter);
			
			unionNormalFormBufferedWriter = new BufferedWriter(unionNormalFormWrite);
			unUnionNormalFormBufferedWriter = new BufferedWriter(unUnionNormalFormWrite);
			canNotChangeToUnionFreeBufferedWriter = new BufferedWriter(canNotChangeToUnionFreeWrite);
			unionWithNotFirstBraceBufferedWriter = new BufferedWriter(unionWithNotFirstBraceWrite);
			unionNormalFormWithFilterAndOptionalBufferedWriter = new BufferedWriter(unionNormalFormWithFilterAndOptionalWrite);

			if (whereClauseBegin == -1) {
				if(!isSubQuery) {
					wellDesignedBufferedWriter.write(queryString + "\n");
					wellCount++;
					System.out.println("total: " + totalCount + " wellcount:" + wellCount + " notwellcout:" + notWellCount
						+ " UnionCount:" + unionCount + " subqueryCount: " + subQueryCount);
				}

				return true;
			}

			Pattern subQueryPattern = Pattern.compile("\\{ *SELECT");
			Matcher subQueryMatcher = subQueryPattern.matcher(upperString);
			
			if (subQueryMatcher.find()) {
				if(!isSubQuery) {
					subQueryBufferedWriter.write(queryString + "\n");
					subQueryCount++;
					System.out.println("total: " + totalCount + " wellcount:" + wellCount + " notwellcout:" + notWellCount
						+ " UnionCount:" + unionCount + " subqueryCount: " + subQueryCount);
				}
				
				upperString = rewriteSparql(upperString, new ArrayList<String>());
			}
			
			if(whereClauseBegin != -1) {
				whereClauseEnd = findTheBraceEnd(upperString, whereClauseBegin) - 1;
			} else {
				whereClauseEnd = -1;
			}
//			whereClauseEnd = upperString.lastIndexOf('}');
			
			whereClause = upperString.substring(whereClauseBegin, whereClauseEnd + 1);
			
			upperString = whereClause.toUpperCase();

			Pattern unionPattern = Pattern.compile("\\} *UNION *\\{");
			Matcher unionMatcher = unionPattern.matcher(upperString);
			
			if (unionMatcher.find()) {
				if(isOptionalUnionFree(upperString)) {
					if(!isSubQuery) {						
						canNotChangeToUnionFreeBufferedWriter.write(queryString + "\n");
						canNotChangeToUnionFreeCount ++;
						System.out.println("total: " + totalCount + " unionNormalFormCount:" + unionNormalFormCount
								+ " unUnionNormalForm:" + unUnionNormalFormCount + " canNotChangeToUnionNormalFree:"
								+ canNotChangeToUnionFreeCount + " unionWithNotFirstBrace:"
								+ unionWithNotFirstBraceCount);
						
						return false;
					}
				}
				
				try {
					if (isUnionNormalForm(upperString)) {
						// P UNION Q UNION R
						if (!isSubQuery) {
//							unionNormalFormBufferedWriter.write(queryString + "\n");
//							unionNormalFormCount ++;
//							System.out.println("total: " + totalCount + " unionNormalFormCount:" + unionNormalFormCount
//									+ " unUnionNormalForm:" + unUnionNormalFormCount + " canNotChangeToUnionNormalFree:"
//									+ canNotChangeToUnionFreeCount + " unionWithNotFirstBrace:"
//									+ unionWithNotFirstBraceCount);
//							
//							return false;
							
							if(!isSafe(whereClause)) {
								unWellDesignedBufferedWriter.write(queryString + "\n");
								notWellCount++;
								System.out.println("total: " + totalCount + " wellcount:" + wellCount + " notwellcout:" + notWellCount);
								
								return false;
							}
							
							if(unionNormalFormWellDesigned(whereClause)) {
								wellDesignedBufferedWriter.write(queryString + "\n");
								wellCount++;
								System.out.println("total: " + totalCount + " wellcount:" + wellCount + " notwellcout:" + notWellCount);
								
								return true;
							} else {
								unWellDesignedBufferedWriter.write(queryString + "\n");
								notWellCount++;
								System.out.println("total: " + totalCount + " wellcount:" + wellCount + " notwellcout:" + notWellCount);
								
								return false;
							}
						}
					} else {
						if (!isSubQuery) {
							if(isUnionNormalWithOptionalOrFilter(whereClause)) {
								unionNormalFormWithFilterAndOptionalBufferedWriter.write(queryString + "\n");
								unionNormalFormWithFilterAndOptionalCount ++;								
								System.out
										.println("total: " + totalCount + " unionNormalFormWithFilterAndOptionalCount:"
												+ unionNormalFormWithFilterAndOptionalCount + " unUnionNormalForm:"
												+ unUnionNormalFormCount);
								
								return false;
							} else {
								unUnionNormalFormBufferedWriter.write(queryString + "\n");
								unUnionNormalFormCount++;
								System.out
										.println("total: " + totalCount + " unionNormalFormWithFilterAndOptionalCount:"
												+ unionNormalFormWithFilterAndOptionalCount + " unUnionNormalForm:"
												+ unUnionNormalFormCount);
								
								return false;
							}
							
//							unUnionNormalFormBufferedWriter.write(queryString + "\n");
//							unUnionNormalFormCount++;
//							System.out.println("total: " + totalCount + " unionNormalFormCount:" + unionNormalFormCount
//									+ " unUnionNormalForm:" + unUnionNormalFormCount + " canNotChangeToUnionNormalFree:"
//									+ canNotChangeToUnionFreeCount + " unionWithNotFirstBrace:"
//									+ unionWithNotFirstBraceCount);
//							return false;
						}
					}
				} catch (Exception e) {
					// P AND Q UNION R
					unionWithNotFirstBraceBufferedWriter.write(queryString + "\n");
					unionWithNotFirstBraceCount++;
					System.out.println("total: " + totalCount + " unionNormalFormCount:" + unionNormalFormCount
							+ " unUnionNormalForm:" + unUnionNormalFormCount + " canNotChangeToUnionNormalFree:"
							+ canNotChangeToUnionFreeCount + " unionWithNotFirstBrace:" + unionWithNotFirstBraceCount);

					return false;
				}
			
				if(!isSubQuery) {
					unionBufferedWriter.write(queryString + "\n");
					unionCount++;
					System.out.println("total: " + totalCount + " wellcount:" + wellCount + " notwellcout:" + notWellCount
							+ " UnionCount:" + unionCount + " subqueryCount: " + subQueryCount);
					return false;
				}
			
			}
			
			if(!isSafe(whereClause)) {
				if(!isSubQuery) {		
					unWellDesignedBufferedWriter.write(queryString + "\n");
					notWellCount++;
					System.out.println("total: " + totalCount + " wellcount:" + wellCount + " notwellcout:" + notWellCount
						+ " UnionCount:" + unionCount + " subqueryCount: " + subQueryCount);
				}
				return false;
			}

			if (travelBGP(upperString, upperString)) {
				if(!isSubQuery) {
					wellDesignedBufferedWriter.write(queryString + "\n");
					wellCount++;
					System.out.println("total: " + totalCount + " wellcount:" + wellCount + " notwellcout:" + notWellCount
						+ " UnionCount:" + unionCount + " subqueryCount: " + subQueryCount);
				}
				return true;
			} else {
				if(!isSubQuery) {		
					unWellDesignedBufferedWriter.write(queryString + "\n");
					notWellCount++;
					System.out.println("total: " + totalCount + " wellcount:" + wellCount + " notwellcout:" + notWellCount
						+ " UnionCount:" + unionCount + " subqueryCount: " + subQueryCount);
				}
				return false;
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				unionBufferedWriter.flush();
				wellDesignedBufferedWriter.flush();
				unWellDesignedBufferedWriter.flush();
				subQueryBufferedWriter.flush();
				unionNormalFormBufferedWriter.flush();
				unUnionNormalFormBufferedWriter.flush();
				canNotChangeToUnionFreeBufferedWriter.flush();
				unionWithNotFirstBraceBufferedWriter.flush();
				unionNormalFormWithFilterAndOptionalBufferedWriter.flush();

				unionBufferedWriter.close();
				wellDesignedBufferedWriter.close();
				unWellDesignedBufferedWriter.close();
				subQueryBufferedWriter.close();
				unionNormalFormBufferedWriter.close();
				unUnionNormalFormBufferedWriter.close();
				canNotChangeToUnionFreeBufferedWriter.close();
				unionWithNotFirstBraceBufferedWriter.close();
				unionNormalFormWithFilterAndOptionalBufferedWriter.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		System.out.println("total: " + totalCount + " wellcount:" + wellCount + " notwellcout:" + notWellCount
				+ " UnionCount:" + unionCount + "subqueryCount: " + subQueryCount);
		return true;
	}
	
	private static boolean isOptionalUnionFree(String whereClause) {
		Pattern optionalPattern = Pattern.compile("OPTIONAL *\\{");
		Matcher optionalMatcher = optionalPattern.matcher(whereClause);
		
		Pattern unionPattern = Pattern.compile("\\} *UNION *\\{");
		Matcher innerUnionMatcher = null;
		
		int start = 0, end = 0;
		String bagPattern = null;
		while(optionalMatcher.find(end)) {
			start = optionalMatcher.end() - 1;
			end = findTheBraceEnd(whereClause, start);
			
			bagPattern = whereClause.substring(start, end);
			
			innerUnionMatcher = unionPattern.matcher(bagPattern);
			if(innerUnionMatcher.find())
				return true;
		}
		
		return false;
	}
	
	private static boolean isUnionNormalForm(String whereClause) throws Exception {
		Pattern firstBracePattern = Pattern.compile("\\{ *\\{");
		Matcher firstBraceMatcher = firstBracePattern.matcher(whereClause);
		
		Pattern unionPattern = Pattern.compile("\\} *UNION *\\{");
		Matcher unionMatcher = unionPattern.matcher(whereClause);
		Matcher innerUnionMatcher = null;
		
		int start = 0, end = 0;
		String bagPattern = null;
		if(firstBraceMatcher.lookingAt()) {
			start = firstBraceMatcher.end() - 1;
			end = findTheBraceEnd(whereClause, start);
			
			bagPattern = whereClause.substring(start, end);
			
			innerUnionMatcher = unionPattern.matcher(bagPattern);
			if(innerUnionMatcher.find())
				return false;
			
			while (unionMatcher.find(end-1)) {
				start = unionMatcher.end() - 1;
				end = findTheBraceEnd(whereClause, start);
				
				bagPattern = whereClause.substring(start, end);
				
				innerUnionMatcher = unionPattern.matcher(bagPattern);
				if(innerUnionMatcher.find())
					return false;
			}
		} else {
			throw new Exception("Union not with left brace");
		}
		
		
		Pattern endPattern = Pattern.compile("^ *\\}?$");
		Matcher endMatcher = endPattern.matcher(whereClause.substring(end));

		if(endMatcher.find())
			return true;
		else
			return false;
	}
	
	private static boolean isUnionNormalWithOptionalOrFilter(String whereClause) {
		Pattern firstBracePattern = Pattern.compile("\\{ *\\{");
		Matcher firstBraceMatcher = firstBracePattern.matcher(whereClause);
		
		Pattern unionPattern = Pattern.compile("\\} *UNION *\\{");
		Matcher unionMatcher = unionPattern.matcher(whereClause);
		Matcher innerUnionMatcher = null;
		
		int start = 0, end = 0;
		String bagPattern = null;
		if(firstBraceMatcher.lookingAt()) {
			start = firstBraceMatcher.end() - 1;
			end = findTheBraceEnd(whereClause, start);
			
			bagPattern = whereClause.substring(start, end);
			
			innerUnionMatcher = unionPattern.matcher(bagPattern);
			if(innerUnionMatcher.find())
				return false;
			
			while (unionMatcher.find(end-1)) {
				start = unionMatcher.end() - 1;
				end = findTheBraceEnd(whereClause, start);
				
				bagPattern = whereClause.substring(start, end);
				
				innerUnionMatcher = unionPattern.matcher(bagPattern);
				if(innerUnionMatcher.find())
					return false;
			}
		} else {
			try {
				throw new Exception("Union not with left brace");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		while(whereClause.charAt(end) == ' ')
			end ++;
		
		if(whereClause.charAt(end) == '.')
			end ++;
		
		int temp;
		Pattern optionalAndFilterPattern = Pattern.compile("( *OPTIONAL *\\{)|( *[\\}]?FILTER[ a-zA-Z]*[\\(\\{])");
		Matcher optionalAndFilterMatcher = optionalAndFilterPattern.matcher(whereClause);
		while (optionalAndFilterMatcher.find(end)) {
			start = optionalAndFilterMatcher.start();
			temp = optionalAndFilterMatcher.end();
			if(whereClause.charAt(temp - 1) == '{')
				end = findTheBraceEnd(whereClause, temp - 1);
			else if(whereClause.charAt(temp - 1) == '(')
				end = findTheLittleBraceEnd(whereClause, temp - 1);
			else {
				try {
					throw new Exception("It is not the correct brace.");
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			while(whereClause.charAt(end) == ' ')
				end ++;
			
			if(whereClause.charAt(end) == '.')
				end ++;
		}		
		
		Pattern endPattern = Pattern.compile("^ *\\}?$");
		Matcher endMatcher = endPattern.matcher(whereClause.substring(end));

		if(endMatcher.find())
			return true;
		else
			return false;
	}
	
	private static boolean unionNormalFormWellDesigned(String whereClause) throws Exception {
		Pattern firstBracePattern = Pattern.compile("\\{ *\\{");
		Matcher firstBraceMatcher = firstBracePattern.matcher(whereClause);
		
		Pattern unionPattern = Pattern.compile("\\} *UNION *\\{");
		Matcher unionMatcher = unionPattern.matcher(whereClause);
		
		int start = 0, end = 0;
		String bagPattern = null;
		if(firstBraceMatcher.lookingAt()) {
			start = firstBraceMatcher.end() - 1;
			end = findTheBraceEnd(whereClause, start);
			
			bagPattern = whereClause.substring(start, end);
			if(!travelBGP(bagPattern, whereClause))
				return false;
			while (unionMatcher.find(end-1)) {
				start = unionMatcher.end() - 1;
				end = findTheBraceEnd(whereClause, start);
				
				bagPattern = whereClause.substring(start, end);
				
				if(!travelBGP(bagPattern, whereClause))
					return false;
			}
		} else {
			throw new Exception("Union not with left brace");
		}
		
		
		Pattern endPattern = Pattern.compile("^ *\\}?$");
		Matcher endMatcher = endPattern.matcher(whereClause.substring(end));

		if(endMatcher.find())
			return true;
		else
			return false;
	}
	
	private static boolean unionNormalFormWithFilterAndOptionalWellDesigned(String whereClause) throws Exception {
		return true;
	}
	
	private static String rewriteSparql(String sourceSparql, ArrayList<String> outerSelectVariables) {
		StringBuffer result = new StringBuffer(sourceSparql);
		
		Pattern selectVariablesPattern = Pattern.compile("(SELECT|CONSTRUCT)[^\\{]*WHERE");
		Matcher selectVariablesMatcher = selectVariablesPattern.matcher(result);
		
		Pattern leftBracePattern = Pattern.compile(" *\\{");
		Matcher leftBraceMatcher = null;
		
		int index = 0, left = 0, right = 0;
		String selectVariablesString = null;
		ArrayList<String> selectVariables = outerSelectVariables;
		
		if (selectVariablesMatcher.find()) {
			selectVariablesString = selectVariablesMatcher.group();			
			selectVariables.addAll(getVariables(selectVariablesString));

			leftBraceMatcher = leftBracePattern.matcher(result.toString());
			if(leftBraceMatcher.find(selectVariablesMatcher.end())) {
				left = leftBraceMatcher.end()-1;
				right = findTheBraceEnd(sourceSparql, left);
			}
			
			char tempChar, c='`';
			boolean flag = false;
			StringBuffer sb = new StringBuffer("");
			for(index=left; index<right; index++) {
				tempChar = result.charAt(index);
				if (!flag) {
					if (tempChar == '?') {
						flag = true;
						sb = new StringBuffer("");
					}
				} else {
					if ((tempChar >= 'a' && tempChar <= 'z') || (tempChar >= 'A' && tempChar <= 'Z')
							|| (tempChar >= '0' && tempChar <= '9') || tempChar == '_' || tempChar == '`')
						sb.append(tempChar);
					else {
						flag = false;
						
						if (!selectVariables.contains(sb.toString())) {
							result.insert(index, c);
							right++;
						}
					}

				}
			}
		}
		
		int begin, end = 0, whereIndex;
		
		String returnString = "";
		StringBuffer selectString = null;
		
		if(selectVariablesMatcher.find(left)) {
			begin = selectVariablesMatcher.start();
//			returnString = result.substring(0, begin);
			leftBraceMatcher = leftBracePattern.matcher(result.toString());
			while(selectVariablesMatcher.find(begin)) {
				if(leftBraceMatcher.find(selectVariablesMatcher.end())) {
					returnString += result.substring(end, selectVariablesMatcher.start());
					whereIndex = leftBraceMatcher.end()-1;
					end = findTheBraceEnd(result.toString(), whereIndex);
				}
				selectString = new StringBuffer(rewriteSparql(result.substring(selectVariablesMatcher.start(), end), selectVariables));
				returnString += selectString;
				begin = end;
			}
			
			returnString += result.substring(begin);
			
			return returnString.toString();
		} else {
			return result.toString();
		}
	}

	private static int findTheLittleBraceEnd(String bagPatterns, int start) {
		int end = bagPatterns.length();

		char tempChar;
		int i, depth = 1;

		if (bagPatterns.charAt(start) != '(')
			try {
				throw new Exception("There is something error in the substring.");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		for (i = start + 1; i < end; i++) {
			if (depth == 0)
				break;

			tempChar = bagPatterns.charAt(i);
			if (tempChar == '(') {
				depth++;
			} else if (tempChar == ')') {
				depth--;
			}
		}

		return i;
	}

	private static int findTheBraceEnd(String bagPatterns, int start) {
		int end = bagPatterns.length();

		char tempChar;
		int i, depth = 1;

		if (bagPatterns.charAt(start) != '{')
			try {
				throw new Exception("There is something error in the substring.");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		for (i = start + 1; i < end; i++) {
			if (depth == 0)
				break;

			tempChar = bagPatterns.charAt(i);
			if (tempChar == '{') {
				depth++;
			} else if (tempChar == '}') {
				depth--;
			}
		}

		return i;
	}
	
	private static int findTheBracePre(String bagPatterns, int start) {
		char tempChar;
		int i, depth = 1;

		if (bagPatterns.charAt(start) != '}')
			try {
				throw new Exception("There is something error in the substring.");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		for (i = start - 1; i >= 0; i--) {
			if (depth == 0)
				break;

			tempChar = bagPatterns.charAt(i);
			if (tempChar == '}') {
				depth++;
			} else if (tempChar == '{') {
				depth--;
			}
		}

		return i;
	}

	private static int findTheBraceEndWithOptionalAndFilter(String bagPatterns, int start, String wholeWhereClause) {
		int end = bagPatterns.length();

		Pattern optionalAndFilterPattern = Pattern.compile("( *OPTIONAL *\\{)|( *[ \\}]?FILTER[ a-zA-Z]*[\\(\\{])");
		Matcher matcher = null;

		start = findTheBraceEnd(bagPatterns, start);
		
		try {
		while (bagPatterns.charAt(start) == ' ') {
			start ++;
		}
		
		if(bagPatterns.charAt(start) == '.') {
			start ++;
		}
		}catch (Exception e) {
			e.printStackTrace();
		}
		
		while (start < end) {
			matcher = optionalAndFilterPattern.matcher(bagPatterns.substring(start));

			if (matcher.lookingAt()) {
				if (bagPatterns.charAt(start + matcher.end() - 1) == '{')
					start = findTheBraceEnd(bagPatterns, start + matcher.end() - 1);
				else if (bagPatterns.charAt(start + matcher.end() - 1) == '(') {
					start = findTheLittleBraceEnd(bagPatterns, start + matcher.end() - 1);
				} else {
					try {
						throw new Exception("It is not the correct brace.");
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				
				while (bagPatterns.charAt(start) == ' ') {
					start ++;
				}
				
				if(bagPatterns.charAt(start) == '.') {
					start ++;
				}
			} else {
				break;
			}
		}
		
//		while (start < end) {
//			matcher = optionalAndFilterPattern.matcher(bagPatterns.substring(start + 1));
//
//			if (matcher.find()) {
//				if(bagPatterns.charAt(start + matcher.end()) == '{')
//					start = findTheBraceEnd(bagPatterns, start + matcher.end());
//				else if(bagPatterns.charAt(start + matcher.end()) == '(') {
//					start = findTheLittleBraceEnd(bagPatterns, start + matcher.end());
//				} else {
//					throw new Exception("It is not the correct brace.");
//				}
//			} else {
//				break;
//			}
//		}

		return start;
	}

	private static ArrayList<String> getVariables(String string) {
		char tempChar;
		int i = 0, length = string.length();
		boolean flag = false;
		StringBuffer sb = new StringBuffer("");
		ArrayList<String> variablesList = new ArrayList<String>();
		for (i = 0; i < length; i++) {
			tempChar = string.charAt(i);
			if (!flag) {
				if (tempChar == '?') {
					if(i > 0) {
						if((string.charAt(i-1)>='A') && (string.charAt(i-1)<='Z')) {
							continue;
						}
					}
					
					flag = true;
					sb = new StringBuffer("");
				}
			} else {
				if ((tempChar >= 'a' && tempChar <= 'z') || (tempChar >= 'A' && tempChar <= 'Z')
						|| (tempChar >= '0' && tempChar <= '9') || tempChar == '_' || tempChar == '`')
					sb.append(tempChar);
				else {
					flag = false;
					if(!variablesList.contains(sb.toString()))
						variablesList.add(new String(sb));
				}

			}
		}

		return variablesList;
	}
	
	private static boolean isSafe(String whereClause) {
		Pattern filterPattern = null;
		Matcher filterMatcher = null;
		
		filterPattern = Pattern.compile("[ \\}]?FILTER[ a-zA-Z]*[\\(\\{]");
		filterMatcher = filterPattern.matcher(whereClause);
		
		// P FILTER Q
		int Pstart = 0, Pend = 0, Qstart = 0, Qend = 0, position = 0;
		String Pstring, Qstring;
		ArrayList<String> filterVariablesList = null;
		ArrayList<String> PVariablesList = null;
		while (filterMatcher.find(position)) {
			Qstart = filterMatcher.end() - 1;
			
			if(whereClause.charAt(Qstart) == '(')
				Qend = findTheLittleBraceEnd(whereClause, Qstart);
			else if(whereClause.charAt(Qstart) == '{')
				Qend = findTheBraceEnd(whereClause, Qstart);
			else {
				try {
					throw new Exception("Error with brace");
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			Qstring = whereClause.substring(Qstart, Qend);
			
			int depth = 1, length = whereClause.length();
			Pend = Qend;
			while (Pend < length) {
				if(whereClause.charAt(Pend) == '{')
					depth ++;
				else if(whereClause.charAt(Pend) == '}')
					depth --;
				
				Pend ++;
				
				if(depth == 0)
					break;
			}
			
			Pstart = findTheBracePre(whereClause, Pend-1);
			Pstart ++;
			
			Pstring = "";
			Pstring += whereClause.substring(Pstart, Qstart);
			
			filterVariablesList = getVariables(Qstring);
			PVariablesList = getVariables(Pstring);
			
//			if (filterVariablesList.contains(",")) {
//				System.out.println("heha");
//			}
			
			if(!PVariablesList.containsAll(filterVariablesList))
				return false;
			
			position = Qstart;
		}
		
		return true;
	}

//	private static boolean isSafe(String bagPattern, ArrayList<Integer> optionalList) {
//		Pattern filterPattern = null;
//		Matcher filterMatcher = null;
//
//		filterPattern = Pattern.compile("FILTER[^\\(\\{]*[\\(\\{]");
//		filterMatcher = filterPattern.matcher(bagPattern);
//
//		int start, end, position = 0;
//		boolean flag = true;
//		Iterator<Integer> iterator = null;
//		ArrayList<Integer> filterList = new ArrayList<Integer>();
//		while (filterMatcher.find(position)) {
//			position = filterMatcher.end() - 1;
//
//			iterator = optionalList.iterator();
//			while (iterator.hasNext()) {
//				start = iterator.next();
//				end = iterator.next();
//
//				if (position > start && position < end) {
//					flag = false;
//				}
//			}
//
//			if (flag) {
//				filterList.add(position);
//				if(bagPattern.charAt(position) == '(')
//					position = findTheLittleBraceEnd(bagPattern, position);
//				else if(bagPattern.charAt(position) == '{')
//					position = findTheBraceEnd(bagPattern, position);
//				else {
//					try {
//						throw new Exception("Error with brace");
//					} catch (Exception e) {
//						// TODO Auto-generated catch block
//						e.printStackTrace();
//					}
//				}
//				filterList.add(position);
//			}
//		}
//
//		iterator = filterList.iterator();
//		ArrayList<String> filterVariablesList = null;
//		ArrayList<String> outFilterVariablesList = null;
//		while (iterator.hasNext()) {
//			filterVariablesList = new ArrayList<String>();
//			outFilterVariablesList = new ArrayList<String>();
//
//			start = iterator.next();
//			end = iterator.next();
//
//			filterVariablesList.addAll(getVariables(bagPattern.substring(start, end)));
//			outFilterVariablesList.addAll(getVariables(bagPattern.substring(0, start)));
//
//			if (!outFilterVariablesList.containsAll(filterVariablesList))
//				return false;
//		}
//
//		return true;
//	}
	
	private static String repalceSubQueryWithVariables(String bagPatterns) {
		// to do: process subquery
		Pattern subQueryPattern = Pattern.compile("\\{ *SELECT");
		Matcher subQueryMatcher = subQueryPattern.matcher(bagPatterns);
		int begin = 0;
		while (subQueryMatcher.find(begin)) {

			int subStart = subQueryMatcher.start();
			int subEnd = findTheBraceEnd(bagPatterns, subStart);
			String subQueryString = null;

			subQueryString = bagPatterns.substring(subStart, subEnd).trim();

			if (!isWelldesign(subQueryString.substring(1, subQueryString.length() - 1), true)) {
				return "-1";
			}

			ArrayList<String> variables = getVariables(subQueryString);

			Iterator<String> iterator = variables.iterator();
			String variable = null;
			String replaceString = "";

			if (subStart > 0) {
				replaceString += bagPatterns.substring(0, subStart);
			}

			// {{subquery} opt {}}
			// replace subquery with its variables
			replaceString += "{";
			while (iterator.hasNext()) {
				variable = iterator.next();
				replaceString += (" ?" + variable + " ");
			}

			replaceString += "}";

			begin = replaceString.length();

			if (subEnd < bagPatterns.length()) {
				replaceString += bagPatterns.substring(subEnd, bagPatterns.length());
			}

			bagPatterns = replaceString;

			subQueryMatcher = subQueryPattern.matcher(bagPatterns);
		}
		
		return bagPatterns;
	}

	private static boolean isInnerOptionalWellDesigned(String bagPattern, String wholeWhereClause) {	
		Pattern innerOptionalPattern = null;
		Matcher innerOptionalMatcher = null;

		innerOptionalPattern = Pattern.compile("OPTIONAL *\\{");
		innerOptionalMatcher = innerOptionalPattern.matcher(bagPattern);

		int position = 0;
		ArrayList<Integer> optionalList = new ArrayList<Integer>();
		while (innerOptionalMatcher.find(position)) {
			position = innerOptionalMatcher.end() - 1;
			optionalList.add(position);
			position = findTheBraceEnd(bagPattern, position);
			optionalList.add(position);
		}

//		if (!isSafe(bagPattern, optionalList))
//			return false;

		int start, end;
		Iterator<Integer> iterator = null;
		iterator = optionalList.iterator();
		while (iterator.hasNext()) {
			start = iterator.next();
			end = iterator.next();

			try {
				if (!travelBGP(bagPattern.substring(start, end), wholeWhereClause))
					return false;
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			// process P = Q OPT R
			String Pstring, Qstring, Rstring, outString;

			Pstring = bagPattern.substring(0, end);
			Qstring = bagPattern.substring(0, start);
			Rstring = bagPattern.substring(start, end);

			int before = wholeWhereClause.indexOf(Pstring);
			int after = before + Pstring.length();

			outString = "";
			if(before != -1)
				outString += wholeWhereClause.substring(0, before);
			if(after < wholeWhereClause.length())
				outString += wholeWhereClause.substring(after);

			ArrayList<String> insideRVariables = new ArrayList<String>();
			ArrayList<String> outsideVariables = new ArrayList<String>();
			ArrayList<String> insideQVariables = new ArrayList<String>();

			insideRVariables = getVariables(Rstring);
			outsideVariables = getVariables(outString);
			insideQVariables = getVariables(Qstring);

			String tempString = null;
			Iterator<String> iteratorString = null;
			iteratorString = insideRVariables.iterator();
			while (iteratorString.hasNext()) {
				tempString = iteratorString.next();
				if (outsideVariables.contains(tempString))
					if (!insideQVariables.contains(tempString))
						return false;
			}
		}

		return true;
	}

	private static boolean isOuterOptionalWellDesigned(String bagPattern, String wholeWhereClause) {
		Pattern subQueryPattern = Pattern.compile("\\{ *SELECT");
		Matcher subQueryMatcher = subQueryPattern.matcher(bagPattern);
		
		if(subQueryMatcher.find()) {
			int bagPatternStart = wholeWhereClause.indexOf(bagPattern);
			int bagPatternEnd = bagPatternStart + bagPattern.length();
			
			bagPattern = repalceSubQueryWithVariables(bagPattern);
			
			if(bagPattern.equals("-1"))
				return false;
			
			String newWholeWhereClause = "";
			if(bagPatternStart > 0) 
				newWholeWhereClause += wholeWhereClause.substring(0, bagPatternStart);
			newWholeWhereClause += bagPattern;
			if(bagPatternEnd < wholeWhereClause.length() - 1)
				newWholeWhereClause += wholeWhereClause.substring(bagPatternEnd, wholeWhereClause.length());
			
			wholeWhereClause = newWholeWhereClause;
		}
		
		int oneBgpEnd = findTheBraceEnd(bagPattern, 0);
		
		int position, start, end;

		if (oneBgpEnd < bagPattern.length()) {
			StringBuffer sbTemp = new StringBuffer(bagPattern);

			Pattern optionalAndFilterPattern = Pattern.compile("[ \\}]?FILTER[ a-zA-Z]*[\\(\\{]");
			Matcher matcher = null;

			start = oneBgpEnd;
			position = start - 1;

			while (bagPattern.charAt(start) == ' ') {
				start++;
			}

			if (bagPattern.charAt(start) == '.') {
				start++;
			}

			end = bagPattern.length();

			while (start < end) {
				matcher = optionalAndFilterPattern.matcher(bagPattern.substring(start));

				if (matcher.lookingAt()) {
					if (sbTemp.charAt(start + matcher.end() - 1) == '{')
						start = findTheBraceEnd(sbTemp.toString(), start + matcher.end() - 1);
					else if (sbTemp.charAt(start + matcher.end() - 1) == '(') {
						start = findTheLittleBraceEnd(sbTemp.toString(), start + matcher.end() - 1);

						if(start < end) {
						while (bagPattern.charAt(start) == ' ') {
							start++;
						}

						if (bagPattern.charAt(start) == '.') {
							start++;
						}
						}

						sbTemp.deleteCharAt(position);
						sbTemp.insert(start-1, '}');
						position = start-1;
					} else {
						try {
							throw new Exception("It is not the correct brace.");
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				} else {
					break;
				}
			}

			bagPattern = sbTemp.toString();

			oneBgpEnd = findTheBraceEnd(bagPattern, 0);
		}
		
		if (!isInnerOptionalWellDesigned(bagPattern.substring(0, oneBgpEnd), wholeWhereClause))
			return false;

		Pattern outerOptionalPattern = null;
		Matcher outerOptionalMatcher = null;

		outerOptionalPattern = Pattern.compile("\\} *OPTIONAL *\\{");
		outerOptionalMatcher = outerOptionalPattern.matcher(bagPattern);

		String Pstring, Qstring, Rstring, outString;

		ArrayList<String> insideRVariables = new ArrayList<String>();
		ArrayList<String> outsideVariables = new ArrayList<String>();
		ArrayList<String> insideQVariables = new ArrayList<String>();

		position = oneBgpEnd-1;
		while (outerOptionalMatcher.find(position)) {
			start = outerOptionalMatcher.end() - 1;
			end = findTheBraceEnd(bagPattern, start);

			position = end - 1;

			if (!travelBGP(bagPattern.substring(start, end), wholeWhereClause))
				return false;

			Pstring = bagPattern.substring(0, end);
			Qstring = bagPattern.substring(0, start);
			Rstring = bagPattern.substring(start, end);

			int before = wholeWhereClause.indexOf(Pstring);
			int after = before + Pstring.length();

			outString = wholeWhereClause.substring(0, before) + wholeWhereClause.substring(after);

			insideRVariables = getVariables(Rstring);
			outsideVariables = getVariables(outString);
			insideQVariables = getVariables(Qstring);

			String tempString = null;
			Iterator<String> iteratorString = null;
			iteratorString = insideRVariables.iterator();
			while (iteratorString.hasNext()) {
				tempString = iteratorString.next();
				if (outsideVariables.contains(tempString))
					if (!insideQVariables.contains(tempString))
						return false;
			}
		}

		return true;
	}

	private static boolean travelBGP(String bagPatterns, String wholeWhereClause) {
		Pattern firstPattern = null;
		Pattern andPattern = null;

		firstPattern = Pattern.compile("\\{ *\\{");
		// unionAndPattern = Pattern.compile("( *UNION *\\{)|( *\\{)");
		andPattern = Pattern.compile(" *\\{");

		Matcher firstMatcher = null;
		Matcher andMatcher = null;

		boolean isWellDesign = true;

		firstMatcher = firstPattern.matcher(bagPatterns);

		if (!firstMatcher.lookingAt()) {
			try {
				isWellDesign = isOuterOptionalWellDesigned(bagPatterns, wholeWhereClause);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			int start = 1, end = 0, cut = 0;

//			andMatcher = andPattern.matcher(bagPatterns.substring(1));
//			while (andMatcher.lookingAt()) {
//				start += andMatcher.end();
//				end = findTheBraceEndWithOptionalAndFilter(bagPatterns, start - 1, wholeWhereClause);
//				isWellDesign = isWellDesign
//						&& travelBGP(bagPatterns.substring(start - 1, end).trim(), wholeWhereClause);
//				start = end;
//				andMatcher = andPattern.matcher(bagPatterns.substring(start));
//			}
			
			andMatcher = andPattern.matcher(bagPatterns.substring(start));
			cut = start;
			while(andMatcher.lookingAt()) {
				start = cut + andMatcher.end() - 1;
				try {
					end = findTheBraceEndWithOptionalAndFilter(bagPatterns, start, wholeWhereClause);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				isWellDesign = isWellDesign
						&& travelBGP(bagPatterns.substring(start, end).trim(), wholeWhereClause);
				start = end;
				cut = start;
				andMatcher = andPattern.matcher(bagPatterns.substring(start));
			}
			
			int deleteBefore = 0;
			
			Pattern endPattern = Pattern.compile("^ *\\}?$");
			Pattern deleteBrace = Pattern.compile(" *(\\.)? *[\\?\\<\\}]");
			Matcher endMatcher = endPattern.matcher(bagPatterns.substring(start));
			Matcher deleteBraceMatcher = deleteBrace.matcher(bagPatterns.substring(start));
			if(!endMatcher.find()) {
				if(deleteBraceMatcher.lookingAt()) {
					deleteBefore = wholeWhereClause.indexOf(bagPatterns);
					
					StringBuffer sbTemp = new StringBuffer(bagPatterns);
					start --;
					while (bagPatterns.charAt(start) != '}') {
						start --;
					}
					
					int leftbrace = findTheBracePre(bagPatterns, start);
					
//					if(leftbrace == -1)
//						try {
//							throw new Exception("left brace wrong");
//						} catch (Exception e) {
//							// TODO Auto-generated catch block
//							e.printStackTrace();
//						}
					
					sbTemp.setCharAt(leftbrace + 1, ' ');
					sbTemp.setCharAt(start, ' ');
					
					StringBuffer tempWholeWhereClause = new StringBuffer(wholeWhereClause);
					tempWholeWhereClause.setCharAt(deleteBefore + leftbrace + 1, ' ');
					tempWholeWhereClause.setCharAt(deleteBefore + start, ' ');
					
					return travelBGP(sbTemp.toString(), tempWholeWhereClause.toString());
				}
				
				try {
					throw new Exception("Not to the end");
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

		return isWellDesign;
	}
}