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
	private static int unionWithoutOptionalAndFilterClauseCount = 0;

	public static boolean isWelldesign(String queryString) {
		String whereClause = null;
		String upperString = queryString.toUpperCase();

		if (upperString.contains("GRAPH "))
			return false;

		if (upperString.contains("MINUS"))
			return false;

		totalCount++;

		int whereClauseBegin, whereClauseEnd;

		whereClauseBegin = upperString.indexOf('{', upperString.indexOf("WHERE"));

		BufferedWriter wellDesignedBufferedWriter = null;
		BufferedWriter unWellDesignedBufferedWriter = null;
		BufferedWriter unionBufferedWriter = null;
		BufferedWriter subQueryBufferedWriter = null;

		try {
			FileWriter wellDesignedFileWriter = new FileWriter("/home/hanxingwang/Data/SearchResult/WellDesigned",
					true);
			FileWriter unWellDesignedfileWriter = new FileWriter("/home/hanxingwang/Data/SearchResult/UnWellDesigned",
					true);
			FileWriter unionFileWriter = new FileWriter("/home/hanxingwang/Data/SearchResult/NotUnionFree", true);
			FileWriter subQueryWriter = new FileWriter("/home/hanxingwang/Data/SearchResult/subQuery", true);

			wellDesignedBufferedWriter = new BufferedWriter(wellDesignedFileWriter);
			unWellDesignedBufferedWriter = new BufferedWriter(unWellDesignedfileWriter);
			unionBufferedWriter = new BufferedWriter(unionFileWriter);
			subQueryBufferedWriter = new BufferedWriter(subQueryWriter);

			if (whereClauseBegin == -1) {
				wellDesignedBufferedWriter.write(queryString + "\n");
				wellCount++;
				System.out.println("total: " + totalCount + " wellcount:" + wellCount + " notwellcout:" + notWellCount
						+ " UnionCount:" + unionCount + " subqueryCount: " + subQueryCount);

				return true;
			}

			Pattern subQueryPattern = Pattern.compile("\\{ *SELECT");
			Matcher subQueryMatcher = subQueryPattern.matcher(upperString);

			if (subQueryMatcher.find()) {
				subQueryBufferedWriter.write(queryString + "\n");
				subQueryCount++;
				System.out.println("total: " + totalCount + " wellcount:" + wellCount + " notwellcout:" + notWellCount
						+ " UnionCount:" + unionCount + " subqueryCount: " + subQueryCount);

				upperString = rewriteSelectSparql(upperString, new ArrayList<String>());
			}

			if (whereClauseBegin != -1) {
				whereClauseEnd = findTheBraceEnd(upperString, whereClauseBegin) - 1;
			} else {
				whereClauseEnd = -1;
			}
			// whereClauseEnd = upperString.lastIndexOf('}');

			whereClause = upperString.substring(whereClauseBegin, whereClauseEnd + 1);

			whereClause = filterNormalForm(whereClause);

			if (!isSafe(whereClause)) {
				unWellDesignedBufferedWriter.write(queryString + "\n");
				notWellCount++;
				System.out.println("total: " + totalCount + " wellcount:" + wellCount + " notwellcout:" + notWellCount
						+ " UnionCount:" + unionCount + " subqueryCount: " + subQueryCount);

				return false;
			}

			upperString = whereClause.toUpperCase();

			Pattern unionPattern = Pattern.compile("\\} *UNION *\\{");
			Matcher unionMatcher = unionPattern.matcher(upperString);

			if (unionMatcher.find()) {
				unionBufferedWriter.write(queryString + "\n");
				unionCount++;
				System.out.println("total: " + totalCount + " wellcount:" + wellCount + " notwellcout:" + notWellCount
						+ " UnionCount:" + unionCount + " subqueryCount: " + subQueryCount);

				if (isUnionWellDesigned(upperString)) {
					wellDesignedBufferedWriter.write(queryString + "\n");
					wellCount++;
					System.out.println("total: " + totalCount + " wellcount:" + wellCount + " notwellcout:"
							+ notWellCount + " UnionCount:" + unionCount + " subqueryCount: " + subQueryCount);

					return true;
				} else {

					unWellDesignedBufferedWriter.write(queryString + "\n");
					notWellCount++;
					System.out.println("total: " + totalCount + " wellcount:" + wellCount + " notwellcout:"
							+ notWellCount + " UnionCount:" + unionCount + " subqueryCount: " + subQueryCount);

					return false;
				}

			}

			if (isOptionalWellDesigned(whereClause)) {
				wellDesignedBufferedWriter.write(queryString + "\n");
				wellCount++;
				System.out.println("total: " + totalCount + " wellcount:" + wellCount + " notwellcout:" + notWellCount
						+ " UnionCount:" + unionCount + " subqueryCount: " + subQueryCount);

				return true;
			} else {
				unWellDesignedBufferedWriter.write(queryString + "\n");
				notWellCount++;
				System.out.println("total: " + totalCount + " wellcount:" + wellCount + " notwellcout:" + notWellCount
						+ " UnionCount:" + unionCount + " subqueryCount: " + subQueryCount);

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

				unionBufferedWriter.close();
				wellDesignedBufferedWriter.close();
				unWellDesignedBufferedWriter.close();
				subQueryBufferedWriter.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		System.out.println("total: " + totalCount + " wellcount:" + wellCount + " notwellcout:" + notWellCount
				+ " UnionCount:" + unionCount + "subqueryCount: " + subQueryCount);
		return true;
	}
	
	private static String filterNormalForm(String oldWhereClause) {
		StringBuffer newWhereClause = null;
		
		if(!oldWhereClause.contains("FILTER"))
			return oldWhereClause;
		
		Pattern filterPattern = null;
		Pattern endPattern = null;
		Matcher filterMatcher = null;
		Matcher beginFilterMatcher = null;
		Matcher endMatcher = null;
		
		filterPattern = Pattern.compile("FILTER[ a-zA-Z]*[\\(\\{]");
		endPattern = Pattern.compile(" *\\}");
		filterMatcher = filterPattern.matcher(oldWhereClause);
		
		// P FILTER Q
		int start, end, leftBrace = 0, rightBrace = 0, index = 0;
		
		while (filterMatcher.find(index)) {
			start = filterMatcher.start();
			
			leftBrace = filterMatcher.end() - 1;
			
			if(oldWhereClause.charAt(leftBrace) == '(')
				rightBrace = findTheLittleBraceEnd(oldWhereClause, leftBrace);
			else if(oldWhereClause.charAt(leftBrace) == '{') 
				rightBrace = findTheBraceEnd(oldWhereClause, leftBrace);
			else {
				try {
					throw new Exception("Error with brace");
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			while (oldWhereClause.charAt(rightBrace) == ' ') {
				rightBrace ++;
			}
			
			if(oldWhereClause.charAt(rightBrace) == '.') {
				rightBrace ++;
			}
			
			end = rightBrace;
			beginFilterMatcher = filterPattern.matcher(oldWhereClause.substring(end).trim());
			while(beginFilterMatcher.lookingAt()) {
				if(!filterMatcher.find(end))
					try {
						throw new Exception("nonononono");
					} catch (Exception e) {
						// TODO: handle exception
					}
				
				leftBrace = filterMatcher.end() - 1;
				
				if(oldWhereClause.charAt(leftBrace) == '(')
					rightBrace = findTheLittleBraceEnd(oldWhereClause, leftBrace);
				else if(oldWhereClause.charAt(leftBrace) == '{') 
					rightBrace = findTheBraceEnd(oldWhereClause, leftBrace);
				else {
					try {
						throw new Exception("Error with brace");
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				
				while (oldWhereClause.charAt(rightBrace) == ' ') {
					rightBrace ++;
				}
				
				if(oldWhereClause.charAt(rightBrace) == '.') {
					rightBrace ++;
				}
				
				end = rightBrace;
				beginFilterMatcher = filterPattern.matcher(oldWhereClause.substring(end).trim());
			}
			
			endMatcher = endPattern.matcher(oldWhereClause.substring(end));
			
			if(endMatcher.lookingAt()) {
				index = end;
				beginFilterMatcher = filterPattern.matcher(oldWhereClause.substring(end));
			} else {
				int depth = 1, length = oldWhereClause.length();
				int position = end;
				
				newWhereClause = new StringBuffer("");
				while (position < length) {
					if(oldWhereClause.charAt(position) == '{')
						depth ++;
					else if(oldWhereClause.charAt(position) == '}')
						depth --;
					
					position ++;
					
					if(depth == 0)
						break;
				}
				
				newWhereClause.append(oldWhereClause.substring(0, start));
				newWhereClause.append(oldWhereClause.substring(end, position-1));
				newWhereClause.append(oldWhereClause.substring(start, end));
				newWhereClause.append(oldWhereClause.substring(position-1));
				
				oldWhereClause = newWhereClause.toString();
				
				beginFilterMatcher = filterPattern.matcher(oldWhereClause.substring(start));
				filterMatcher = filterPattern.matcher(oldWhereClause);
			}		
		}
		
		return oldWhereClause;
	}
	
	private static boolean isUnionWellDesigned(String whereClause) {
		BufferedWriter unionNormalFormBufferedWriter = null;
		BufferedWriter unUnionNormalFormBufferedWriter = null;
		BufferedWriter canNotChangeToUnionFreeBufferedWriter = null;
		BufferedWriter unionWithoutOptionalAndFilterClauseBufferedWriter = null;
//		BufferedWriter theRestBufferedWrite = null;

		try {
			FileWriter unionNormalFormWrite = new FileWriter("/home/hanxingwang/Data/SearchResult/unionNormalForm",
					true);
			FileWriter unUnionNormalFormWrite = new FileWriter("/home/hanxingwang/Data/SearchResult/unUnionNormalForm",
					true);
			FileWriter canNotChangeToUnionFreeWrite = new FileWriter(
					"/home/hanxingwang/Data/SearchResult/canNotChangeToUnionNormalForm", true);
			FileWriter unionWithoutOptionalAndFilterClauseWrite = new FileWriter(
			 "/home/hanxingwang/Data/SearchResult/unionWithoutOptionalAndFilterClause",
			 true);
//			FileWriter theRestWrite = new FileWriter("/home/hanxingwang/Data/SearchResult/TheRest", true);


			unionNormalFormBufferedWriter = new BufferedWriter(unionNormalFormWrite);
			unUnionNormalFormBufferedWriter = new BufferedWriter(unUnionNormalFormWrite);
			canNotChangeToUnionFreeBufferedWriter = new BufferedWriter(canNotChangeToUnionFreeWrite);
			unionWithoutOptionalAndFilterClauseBufferedWriter = new BufferedWriter(unionWithoutOptionalAndFilterClauseWrite);
//			theRestBufferedWrite = new BufferedWriter(theRestWrite);

			// do not have the optional and filter key world
			if (isUnionWithoutOptional(whereClause)) {
				unionWithoutOptionalAndFilterClauseBufferedWriter.write(whereClause + "\n");
				unionWithoutOptionalAndFilterClauseCount++;
				System.out.println("total: " + totalCount + " unionNormalFormCount:" + unionNormalFormCount
						+ " unUnionNormalForm:" + unUnionNormalFormCount + " canNotChangeToUnionNormalFree:"
						+ canNotChangeToUnionFreeCount + " unionWithoutOptionalAndFilterClause:"
						+ unionWithoutOptionalAndFilterClauseCount);

				return true;
			}
			
			if (isOptionalUnionFree(whereClause)) {
				canNotChangeToUnionFreeBufferedWriter.write(whereClause + "\n");
				canNotChangeToUnionFreeCount++;
				System.out.println("total: " + totalCount + " unionNormalFormCount:" + unionNormalFormCount
						+ " unUnionNormalForm:" + unUnionNormalFormCount + " canNotChangeToUnionNormalFree:"
						+ canNotChangeToUnionFreeCount + " unionWithoutOptionalAndFilterClause:"
						+ unionWithoutOptionalAndFilterClauseCount);

				return false;
			}
			
			while(!isUnionNormal(whereClause)) {
				whereClause = rewriteToUnionNormalForm(whereClause);
			}
			
			if(!isUnionNormal(whereClause))
				try {
					throw new Exception("rewrite error!");
				} catch (Exception e) {
						// TODO: handle exception
				}		
				
			if (unionNormalWellDesigned(whereClause))
				return true;
			else
				return false;
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				unionNormalFormBufferedWriter.flush();
				unUnionNormalFormBufferedWriter.flush();
				canNotChangeToUnionFreeBufferedWriter.flush();
				unionWithoutOptionalAndFilterClauseBufferedWriter.flush();
//				theRestBufferedWrite.flush();

				unionNormalFormBufferedWriter.close();
				unUnionNormalFormBufferedWriter.close();
				canNotChangeToUnionFreeBufferedWriter.close();
				unionWithoutOptionalAndFilterClauseBufferedWriter.close();
//				theRestBufferedWrite.close();

			} catch (Exception e) {
				// TODO: handle exception
				e.printStackTrace();
			}
		}

		return true;
	}
	
	// OPT (P UNION Q)
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
	
	// do not have optional and filter key word
	private static boolean isUnionWithoutOptional(String whereClause) {
		Pattern optionalPattern = Pattern.compile("OPTIONAL *\\{");
		Matcher optionalMatcher = optionalPattern.matcher(whereClause);
		
		if(optionalMatcher.find())
			return false;
		else
			return true;
	}
	
	
	// P UNION Q UNION R
	private static boolean isUnionNormal(String whereClause) {
		Pattern firstBracePattern = Pattern.compile("\\{ *\\{");
		Matcher firstBraceMatcher = firstBracePattern.matcher(whereClause);
		
		Pattern unionPattern = Pattern.compile("\\} *UNION *\\{");
		Matcher unionMatcher = unionPattern.matcher(whereClause);
		Matcher beginUnionMatcher = null;
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
			
			beginUnionMatcher = unionPattern.matcher(whereClause.substring(end - 1));
			while (beginUnionMatcher.lookingAt()) {
				if(!unionMatcher.find(end-1))
					try {
						throw new Exception("nonono!");
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				
				start = unionMatcher.end() - 1;
				end = findTheBraceEnd(whereClause, start);
				
				bagPattern = whereClause.substring(start, end);
				
				innerUnionMatcher = unionPattern.matcher(bagPattern);
				if(innerUnionMatcher.find())
					return false;
				
				beginUnionMatcher = unionPattern.matcher(whereClause.substring(end - 1));
			}
		} else {
			return false;
		}
		
		
		Pattern endPattern = Pattern.compile("^ *\\}?$");
		Matcher endMatcher = endPattern.matcher(whereClause.substring(end));

		if(endMatcher.find())
			return true;
		else
			return false;
	}
	
	// P AND UNION-NORMAL-FORM AND Q
	private static String rewriteToUnionNormalForm(String oldWhereClause) {
		StringBuffer newWhereClause = new StringBuffer("");
		
		ArrayList<String> unionList = getUnionList(oldWhereClause);
		
		newWhereClause.append("{");
		if(!unionList.isEmpty()) {
			boolean firstFlag = true;
			
			for(String union : unionList) {				
				if(firstFlag) {
					newWhereClause.append(" " + "{ " + union + " }" + " ");
				} else {
					newWhereClause.append(" UNION " + "{ " + union + " }" + " ");
				}
				
				firstFlag = false;
			}
			
		} else {
			try {
				throw new Exception("Rewrite error in unnormal form.");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		newWhereClause.append("}");
		
		return newWhereClause.toString();
	}
	
	private static ArrayList<String> getUnionList(String bagPatterns) {
		ArrayList<String> unionNormalFormList = new ArrayList<String>();
		
		if(isUnionNormal(bagPatterns)) {
			int index = 1, length = bagPatterns.length(), start, end;
			while(index < length) {
				if(bagPatterns.charAt(index) == '{') {
					start = index;
					end = findTheBraceEnd(bagPatterns, start);
					unionNormalFormList.add(bagPatterns.substring(start + 1, end-1));
					
					index = end;
				} else {
					index ++;
				}
			}
			
			return unionNormalFormList;
		}
		
		Pattern unionPattern = Pattern.compile("\\} *UNION *\\{");
		Matcher unionMatcher = unionPattern.matcher(bagPatterns);
		
		String previousString = null;
		int previousStringEnd = 0;
		if(unionMatcher.find()) {
			int index = unionMatcher.start();
			int last= index + 1;
						
			index ++;
			int depth = 1, length = bagPatterns.length();
			while(index < length) {
				if(depth == 0) {
					depth = 1;
					last = index;
				}
				
				if(bagPatterns.charAt(index) == '{') {
					depth ++;
				} else if(bagPatterns.charAt(index) == '}') {
					depth --;
				}
				
				index ++;
			}
			
			if(index > length)
				try {
					throw new Exception("It is not the correct union bagpatterns.");
				} catch (Exception e) {
					// TODO: handle exception
					e.printStackTrace();
				}
			
			
			previousStringEnd = findTheBracePre(bagPatterns, last-1);
			
			if(previousStringEnd < 0)
				try {
					throw new Exception("It is not the correct union bagpatterns.");
				} catch (Exception e) {
					// TODO: handle exception
					e.printStackTrace();
				}
			
			previousStringEnd ++;
			
			previousString = bagPatterns.substring(1, previousStringEnd).trim();
		} else 
			try {
				throw new Exception("It is not necessory to rewrite this query.");
			} catch (Exception e) {
				// TODO: handle exception
			}
		
		int start, end;
		String bagPattern = null;
		Matcher innerMatcher = null;
		Matcher beginMatcher = null;
		
		start = previousStringEnd;
		end = findTheBraceEnd(bagPatterns, start);
		
		bagPattern = bagPatterns.substring(start, end);
		innerMatcher = unionPattern.matcher(bagPattern);
		if(innerMatcher.find()) {
			unionNormalFormList.addAll(getUnionList(bagPattern));
		} else {
			unionNormalFormList.add(bagPattern.substring(1, bagPattern.length()-1).trim());
		}
		
		beginMatcher = unionPattern.matcher(bagPatterns.substring(end -1));
		unionMatcher = unionMatcher.reset();
		while (beginMatcher.lookingAt()) {
			if(!unionMatcher.find(end-1)) 
				try {
					throw new Exception("nononononono");
				} catch (Exception e) {
					// TODO: handle exception
					e.printStackTrace();
				}
			
			start = unionMatcher.end() - 1;
			end = findTheBraceEnd(bagPatterns, start);
			
			bagPattern = bagPatterns.substring(start, end);
			innerMatcher = unionPattern.matcher(bagPattern);
			if(innerMatcher.find()) {
				unionNormalFormList.addAll(getUnionList(bagPattern));
			} else {
				unionNormalFormList.add(bagPattern.substring(1, bagPattern.length()-1).trim());
			}
			
			beginMatcher = unionPattern.matcher(bagPatterns.substring(end - 1));
		}
		
		while(bagPatterns.charAt(end) == ' ')
			end ++;
		
		if(bagPatterns.charAt(end) == '.')
			end ++;
		
		String afterwardString = bagPatterns.substring(end, bagPatterns.length()-1).trim();
		
		ArrayList<String> unionList = new ArrayList<String>();
		StringBuffer unionClause = null;
		if(!unionNormalFormList.isEmpty()) {
			
			for(String union : unionNormalFormList) {
				unionClause = new StringBuffer("");
				
				if(!previousString.trim().equals(""))
					unionClause.append(" " + previousString + (previousString.endsWith(".") ? " " : " ."));				
				unionClause.append(" " + union + (union.endsWith(".") ? " " : " ."));	
				if(!afterwardString.trim().equals(""))
					unionClause.append(" " + afterwardString);
				
				unionList.add(unionClause.toString());
			}
			
		} else {
			try {
				throw new Exception("Rewrite error in the optional and filter form.");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		return unionList;
	}
	
	private static boolean unionNormalWellDesigned(String whereClause) throws Exception {
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
			if(!isOptionalUnionFree(bagPattern))
				return false;
			while (unionMatcher.find(end-1)) {
				start = unionMatcher.end() - 1;
				end = findTheBraceEnd(whereClause, start);
				
				bagPattern = whereClause.substring(start, end);
				
				if(!isOptionalUnionFree(bagPattern))
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
	
	private static String rewriteSelectSparql(String sourceSparql, ArrayList<String> outerSelectVariables) {
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
					if (tempChar == '?' || tempChar == '$') {
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
				selectString = new StringBuffer(rewriteSelectSparql(result.substring(selectVariablesMatcher.start(), end), selectVariables));
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
		boolean firstChar = false;
		StringBuffer sb = new StringBuffer("");
		ArrayList<String> variablesList = new ArrayList<String>();
		for (i = 0; i < length; i++) {
			tempChar = string.charAt(i);
			if (!flag) {
				if (tempChar == '?' || tempChar == '$') {
					if(i > 0) {
						if((string.charAt(i-1)!=' ') && (string.charAt(i-1)!='(') && (string.charAt(i-1)!='{') && (string.charAt(i-1) != '.')) {
							continue;
						}
					}
					
					flag = true;
					firstChar = true;
					sb = new StringBuffer("");
				}
			} else {
				if(firstChar) {
					if(!(tempChar >= 'A' && tempChar <= 'Z'))
						flag = false;
						firstChar = false;
						continue;
				}
				
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
		if(!whereClause.contains("FILTER"))
			return true;
		
		Pattern filterPattern = null;
		Matcher filterMatcher = null;
		
		filterPattern = Pattern.compile("FILTER[ a-zA-Z]*[\\(\\{]");
		filterMatcher = filterPattern.matcher(whereClause);
		
		// P FILTER Q
		int Pstart = 0, Pend = 0, Qstart = 0, Qend = 0, position = 0;
		ArrayList<Integer> filterPositionList = new ArrayList<Integer>();
		ArrayList<Integer> filterAreaList = new ArrayList<Integer>();
		while (filterMatcher.find(position)) {
			Qstart = filterMatcher.end() - 1;
			filterPositionList.add(Qstart);
			
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
			
			filterPositionList.add(Qend);
			
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
			
			filterAreaList.add(Pstart);
			filterAreaList.add(Pend);
			
			position = Qstart;
		}
		
		int filterCount = filterPositionList.size()/2;
		String Pstring, Qstring;
		ArrayList<String> filterVariablesList = null;
		ArrayList<String> PVariablesList = null;
		int i, j, filterStart;
		for(i=0; i<filterCount; i++) {
			Qstart = filterPositionList.get(2*i);
			Qend = filterPositionList.get(2*i + 1);
			
			Pstart = filterAreaList.get(2*i);
			Pend = filterAreaList.get(2*i + 1);
			
			Qstring = whereClause.substring(Qstart, Qend);
			
			Pstring = "";
			Pstring += whereClause.substring(Pstart, Qstart);
			j = i+1;
			position = Qend;
			while (j < filterCount) {
				filterStart = filterPositionList.get(2*j);
				if(filterStart < Pend && position < filterStart) {
					Pstring += whereClause.substring(position, filterStart);
					position = filterPositionList.get(2*j+1);
					j ++;
				} else {
					break;
				}
			}
			
			Pstring += whereClause.substring(position, Pend);
			
			filterVariablesList = getVariables(Qstring);
			PVariablesList = getVariables(Pstring);
			
//			if (filterVariablesList.contains(",")) {
//				System.out.println("heha");
//			}
			
			if(!PVariablesList.containsAll(filterVariablesList))
				return false;
		}
		
		return true;
	}
	
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

			if (!isWelldesign(subQueryString.substring(1, subQueryString.length() - 1))) {
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
	
	private static boolean isOptionalWellDesigned(String whereClause) {
		if(!whereClause.contains("OPTIONAL"))
			return true;
		
//		Pattern optionalPattern = Pattern
		
		return true;
	}

//	private static boolean isInnerOptionalWellDesigned(String bagPattern, String wholeWhereClause) {	
//		Pattern innerOptionalPattern = null;
//		Matcher innerOptionalMatcher = null;
//
//		innerOptionalPattern = Pattern.compile("OPTIONAL *\\{");
//		innerOptionalMatcher = innerOptionalPattern.matcher(bagPattern);
//
//		int position = 0;
//		ArrayList<Integer> optionalList = new ArrayList<Integer>();
//		while (innerOptionalMatcher.find(position)) {
//			position = innerOptionalMatcher.end() - 1;
//			optionalList.add(position);
//			position = findTheBraceEnd(bagPattern, position);
//			optionalList.add(position);
//		}
//		
////		if (!isSafe(bagPattern, optionalList))
////			return false;
//
//		int start, end;
//		Iterator<Integer> iterator = null;
//		iterator = optionalList.iterator();
//		while (iterator.hasNext()) {
//			start = iterator.next();
//			end = iterator.next();
//
//			try {
//				if (!travelBGP(bagPattern.substring(start, end), wholeWhereClause))
//					return false;
//			} catch (Exception e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//
//			// process P = Q OPT R
//			String Pstring, Qstring, Rstring, outString;
//
//			Pstring = bagPattern.substring(0, end);
//			Qstring = bagPattern.substring(0, start);
//			Rstring = bagPattern.substring(start, end);
//
//			int before = wholeWhereClause.indexOf(Pstring);
//			int after = before + Pstring.length();
//
//			outString = "";
//			if(before != -1)
//				outString += wholeWhereClause.substring(0, before);
//			if(after < wholeWhereClause.length())
//				outString += wholeWhereClause.substring(after);
//
//			ArrayList<String> insideRVariables = new ArrayList<String>();
//			ArrayList<String> outsideVariables = new ArrayList<String>();
//			ArrayList<String> insideQVariables = new ArrayList<String>();
//
//			insideRVariables = getVariables(Rstring);
//			outsideVariables = getVariables(outString);
//			insideQVariables = getVariables(Qstring);
//
//			String tempString = null;
//			Iterator<String> iteratorString = null;
//			iteratorString = insideRVariables.iterator();
//			while (iteratorString.hasNext()) {
//				tempString = iteratorString.next();
//				if (outsideVariables.contains(tempString))
//					if (!insideQVariables.contains(tempString))
//						return false;
//			}
//		}
//
//		return true;
//	}
//
//	private static boolean isOuterOptionalWellDesigned(String bagPattern, String wholeWhereClause) {
//		Pattern subQueryPattern = Pattern.compile("\\{ *SELECT");
//		Matcher subQueryMatcher = subQueryPattern.matcher(bagPattern);
//		
//		if(subQueryMatcher.find()) {
//			int bagPatternStart = wholeWhereClause.indexOf(bagPattern);
//			int bagPatternEnd = bagPatternStart + bagPattern.length();
//			
//			bagPattern = repalceSubQueryWithVariables(bagPattern);
//			
//			if(bagPattern.equals("-1"))
//				return false;
//			
//			String newWholeWhereClause = "";
//			if(bagPatternStart > 0) 
//				newWholeWhereClause += wholeWhereClause.substring(0, bagPatternStart);
//			newWholeWhereClause += bagPattern;
//			if(bagPatternEnd < wholeWhereClause.length() - 1)
//				newWholeWhereClause += wholeWhereClause.substring(bagPatternEnd, wholeWhereClause.length());
//			
//			wholeWhereClause = newWholeWhereClause;
//		}
//		
//		int oneBgpEnd = findTheBraceEnd(bagPattern, 0);
//		
//		int position, start, end;
//
//		if (oneBgpEnd < bagPattern.length()) {
//			StringBuffer sbTemp = new StringBuffer(bagPattern);
//
//			Pattern optionalAndFilterPattern = Pattern.compile(" *[\\}]?FILTER[ a-zA-Z]*[\\(\\{]");
//			Matcher matcher = null;
//
//			start = oneBgpEnd;
//			position = start - 1;
//
//			while (bagPattern.charAt(start) == ' ') {
//				start++;
//			}
//
//			if (bagPattern.charAt(start) == '.') {
//				start++;
//			}
//
//			end = bagPattern.length();
//
//			while (start < end) {
//				matcher = optionalAndFilterPattern.matcher(bagPattern.substring(start));
//
//				if (matcher.lookingAt()) {
//					if (sbTemp.charAt(start + matcher.end() - 1) == '{')
//						start = findTheBraceEnd(sbTemp.toString(), start + matcher.end() - 1);
//					else if (sbTemp.charAt(start + matcher.end() - 1) == '(') {
//						start = findTheLittleBraceEnd(sbTemp.toString(), start + matcher.end() - 1);
//
//						if(start < end) {
//						while (bagPattern.charAt(start) == ' ') {
//							start++;
//						}
//
//						if (bagPattern.charAt(start) == '.') {
//							start++;
//						}
//						}
//
//						sbTemp.deleteCharAt(position);
//						sbTemp.insert(start-1, '}');
//						position = start-1;
//					} else {
//						try {
//							throw new Exception("It is not the correct brace.");
//						} catch (Exception e) {
//							// TODO Auto-generated catch block
//							e.printStackTrace();
//						}
//					}
//				} else {
//					break;
//				}
//			}
//
//			bagPattern = sbTemp.toString();
//
//			oneBgpEnd = findTheBraceEnd(bagPattern, 0);
//		}
//		
//		if (!isInnerOptionalWellDesigned(bagPattern.substring(0, oneBgpEnd), wholeWhereClause))
//			return false;
//
//		Pattern outerOptionalPattern = null;
//		Matcher outerOptionalMatcher = null;
//		Matcher decideOptionalMatcher = null;
//
//		outerOptionalPattern = Pattern.compile("\\} *[\\.]? *OPTIONAL *\\{");
//		outerOptionalMatcher = outerOptionalPattern.matcher(bagPattern);
//
//		String Pstring, Qstring, Rstring, outString;
//
//		ArrayList<String> insideRVariables = new ArrayList<String>();
//		ArrayList<String> outsideVariables = new ArrayList<String>();
//		ArrayList<String> insideQVariables = new ArrayList<String>();
//
//		position = oneBgpEnd-1;
//		while (outerOptionalMatcher.find(position)) {
//			decideOptionalMatcher = outerOptionalPattern.matcher(bagPattern.substring(position));
//			
//			if(!decideOptionalMatcher.lookingAt())
//				System.err.println("hah");
//			
//			
//			start = outerOptionalMatcher.end() - 1;
//			end = findTheBraceEnd(bagPattern, start);
//
//			position = end - 1;
//
//			if (!travelBGP(bagPattern.substring(start, end), wholeWhereClause))
//				return false;
//
//			Pstring = bagPattern.substring(0, end);
//			Qstring = bagPattern.substring(0, start);
//			Rstring = bagPattern.substring(start, end);
//
//			int before = wholeWhereClause.indexOf(Pstring);
//			int after = before + Pstring.length();
//
//			outString = wholeWhereClause.substring(0, before) + wholeWhereClause.substring(after);
//
//			insideRVariables = getVariables(Rstring);
//			outsideVariables = getVariables(outString);
//			insideQVariables = getVariables(Qstring);
//
//			String tempString = null;
//			Iterator<String> iteratorString = null;
//			iteratorString = insideRVariables.iterator();
//			while (iteratorString.hasNext()) {
//				tempString = iteratorString.next();
//				if (outsideVariables.contains(tempString))
//					if (!insideQVariables.contains(tempString))
//						return false;
//			}
//		}
//		
//		Pattern endPattern = Pattern.compile("^\\} *[\\.]? *$");
//		Matcher endMatcher = endPattern.matcher(bagPattern.substring(position));
//		
//		if(!endMatcher.find())
//			System.err.println("nonono");
//		
//
//		return true;
//	}

//	private static boolean travelBGP(String bagPatterns, String wholeWhereClause) {
//		if(!bagPatterns.contains("OPTIONAL"))
//			return true;
//		
//		Pattern firstPattern = null;
//		Pattern andPattern = null;
//
//		firstPattern = Pattern.compile("\\{ *\\{");
//		// unionAndPattern = Pattern.compile("( *UNION *\\{)|( *\\{)");
//		andPattern = Pattern.compile(" *\\{");
//
//		Matcher firstMatcher = null;
//		Matcher andMatcher = null;
//
//		boolean isWellDesign = true;
//
//		firstMatcher = firstPattern.matcher(bagPatterns);
//
//		if (!firstMatcher.lookingAt()) {
//			try {
//				isWellDesign = isOuterOptionalWellDesigned(bagPatterns, wholeWhereClause);
//			} catch (Exception e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//		} else {
//			int start = 1, end = 0, cut = 0;
//			
//			andMatcher = andPattern.matcher(bagPatterns.substring(start));
//			cut = start;
//			while(andMatcher.lookingAt()) {
//				start = cut + andMatcher.end() - 1;
//				try {
//					end = findTheBraceEndWithOptionalAndFilter(bagPatterns, start, wholeWhereClause);
//				} catch (Exception e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
//				isWellDesign = isWellDesign
//						&& travelBGP(bagPatterns.substring(start, end).trim(), wholeWhereClause);
//				start = end;
//				cut = start;
//				andMatcher = andPattern.matcher(bagPatterns.substring(start));
//			}
//			
//			int deleteBefore = 0;
//			
//			Pattern endPattern = Pattern.compile(" *\\}?$");
//			Pattern deleteBrace = Pattern.compile(" *(\\.)? *[\\?\\<\\}]");
//			Matcher endMatcher = endPattern.matcher(bagPatterns.substring(start));
//			Matcher deleteBraceMatcher = deleteBrace.matcher(bagPatterns.substring(start));
//			if(!endMatcher.lookingAt()) {
//				if(deleteBraceMatcher.lookingAt()) {
//					deleteBefore = wholeWhereClause.indexOf(bagPatterns);
//					
//					StringBuffer sbTemp = new StringBuffer(bagPatterns);
//					start --;
//					while (bagPatterns.charAt(start) != '}') {
//						start --;
//					}
//					
//					int leftbrace = findTheBracePre(bagPatterns, start);
//					
//					sbTemp.setCharAt(leftbrace + 1, ' ');
//					sbTemp.setCharAt(start, ' ');
//					
//					if(sbTemp.substring(leftbrace + 1, start).contains("optional"))
//						System.err.println("nononono");
//					
//					StringBuffer tempWholeWhereClause = new StringBuffer(wholeWhereClause);
//					tempWholeWhereClause.setCharAt(deleteBefore + leftbrace + 1, ' ');
//					tempWholeWhereClause.setCharAt(deleteBefore + start, ' ');
//					
//					return travelBGP(sbTemp.toString(), tempWholeWhereClause.toString());
//				}
//				
//				try {
//					throw new Exception("Not to the end");
//				} catch (Exception e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
//			}
//		}
//
//		return isWellDesign;
//	}
}