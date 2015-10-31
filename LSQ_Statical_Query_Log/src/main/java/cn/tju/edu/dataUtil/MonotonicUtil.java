package cn.tju.edu.dataUtil;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MonotonicUtil {
	private static int totalCount = 0;
	private static int monotonicCount = 0;
	private static int nonmonotonicCount = 0;

	public static boolean isMonotonicity(String queryString, boolean isSubQuery) {
		String whereClause = null;
		String upperString = queryString.toUpperCase();

		Pattern minusPattern = Pattern.compile(" *MINUS *");
		Matcher minusMatcher = minusPattern.matcher(upperString);

		if (!isSubQuery)
			totalCount++;

		int whereClauseBegin, whereClauseEnd;

		whereClauseBegin = upperString.indexOf('{', upperString.indexOf("WHERE"));

		BufferedWriter MonotonicBufferedWriter = null;
		BufferedWriter NonmonotonicBufferedWriter = null;

		try {
			FileWriter monotonicFileWriter = new FileWriter("/home/hanxingwang/Data/SearchResult/Monotinic", true);
			FileWriter nonmonotonicFileWriter = new FileWriter("/home/hanxingwang/Data/SearchResult/Nonmonotinic",
					true);

			MonotonicBufferedWriter = new BufferedWriter(monotonicFileWriter);
			NonmonotonicBufferedWriter = new BufferedWriter(nonmonotonicFileWriter);

			if (minusMatcher.find()) {
				NonmonotonicBufferedWriter.write(queryString + "\n");
				nonmonotonicCount++;
				System.out.println("total: " + totalCount + " monotonicCount:" + monotonicCount + " nonmonotonicCount:"
						+ nonmonotonicCount);

				return false;
			}

			if (whereClauseBegin == -1) {
				if (!isSubQuery) {
					MonotonicBufferedWriter.write(queryString + "\n");
					monotonicCount++;
					System.out.println("total: " + totalCount + " monotonicCount:" + monotonicCount
							+ " nonmonotonicCount:" + nonmonotonicCount);
				}

				return true;
			}

			if (whereClauseBegin != -1) {
				whereClauseEnd = findTheBraceEnd(upperString, whereClauseBegin) - 1;
			} else {
				whereClauseEnd = -1;
			}

			whereClause = upperString.substring(whereClauseBegin, whereClauseEnd + 1);
			whereClause = filterNormalForm(whereClause);

			upperString = whereClause.toUpperCase();

			if (!isSafe(whereClause)) {
				NonmonotonicBufferedWriter.write(queryString + "\n");
				nonmonotonicCount++;
				System.out.println("total: " + totalCount + " monotonicCount:" + monotonicCount + " nonmonotonicCount:"
						+ nonmonotonicCount);

				return false;
			}

			Pattern subQueryPattern = Pattern.compile("\\{ *SELECT");
			Matcher subQueryMatcher = subQueryPattern.matcher(whereClause);

			if (subQueryMatcher.find()) {
				whereClause = rewriteSelectSparql(whereClause, new ArrayList<String>());

				if (isSubQueryMonotonic(whereClause)) {
					if (!isSubQuery) {
						MonotonicBufferedWriter.write(queryString + "\n");
						monotonicCount++;
						System.out.println("total: " + totalCount + " monotonicCount:" + monotonicCount
								+ " nonmonotonicCount:" + nonmonotonicCount);
					}

					return true;
				} else {
					if (!isSubQuery) {
						NonmonotonicBufferedWriter.write(queryString + "\n");
						nonmonotonicCount++;
						System.out.println("total: " + totalCount + " monotonicCount:" + monotonicCount
								+ " nonmonotonicCount:" + nonmonotonicCount);
					}

					return false;
				}
			}

			Pattern unionPattern = Pattern.compile("\\} *UNION *\\{");
			Matcher unionMatcher = unionPattern.matcher(whereClause);

			if (unionMatcher.find()) {
				if (isUnionMonotonic(whereClause)) {
					if (!isSubQuery) {
						MonotonicBufferedWriter.write(queryString + "\n");
						monotonicCount++;
						System.out.println("total: " + totalCount + " monotonicCount:" + monotonicCount
								+ " nonmonotonicCount:" + nonmonotonicCount);
					}

					return true;
				} else {
					if (!isSubQuery) {
						NonmonotonicBufferedWriter.write(queryString + "\n");
						nonmonotonicCount++;
						System.out.println("total: " + totalCount + " monotonicCount:" + monotonicCount
								+ " nonmonotonicCount:" + nonmonotonicCount);
					}

					return false;
				}

			}

			if (isOptionalMonotonic(whereClause)) {
				if (!isSubQuery) {
					MonotonicBufferedWriter.write(queryString + "\n");
					monotonicCount++;
					System.out.println("total: " + totalCount + " monotonicCount:" + monotonicCount
							+ " nonmonotonicCount:" + nonmonotonicCount);
				}

				return true;
			} else {
				if (!isSubQuery) {
					NonmonotonicBufferedWriter.write(queryString + "\n");
					nonmonotonicCount++;
					System.out.println("total: " + totalCount + " monotonicCount:" + monotonicCount
							+ " nonmonotonicCount:" + nonmonotonicCount);
				}

				return false;
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				MonotonicBufferedWriter.flush();
				NonmonotonicBufferedWriter.flush();

				MonotonicBufferedWriter.close();
				NonmonotonicBufferedWriter.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		System.out.println("total: " + totalCount + " monotonicCount:" + monotonicCount + " nonmonotonicCount:"
				+ nonmonotonicCount);
		return true;
	}

	private static String filterNormalForm(String oldWhereClause) {
		StringBuffer newWhereClause = null;

		if (!oldWhereClause.contains("FILTER"))
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

			if (oldWhereClause.charAt(leftBrace) == '(')
				rightBrace = findTheLittleBraceEnd(oldWhereClause, leftBrace);
			else if (oldWhereClause.charAt(leftBrace) == '{')
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
				rightBrace++;
			}

			if (oldWhereClause.charAt(rightBrace) == '.') {
				rightBrace++;
			}

			end = rightBrace;
			beginFilterMatcher = filterPattern.matcher(oldWhereClause.substring(end).trim());
			while (beginFilterMatcher.lookingAt()) {
				if (!filterMatcher.find(end))
					try {
						throw new Exception("nonononono");
					} catch (Exception e) {
						// TODO: handle exception
					}

				leftBrace = filterMatcher.end() - 1;

				if (oldWhereClause.charAt(leftBrace) == '(')
					rightBrace = findTheLittleBraceEnd(oldWhereClause, leftBrace);
				else if (oldWhereClause.charAt(leftBrace) == '{')
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
					rightBrace++;
				}

				if (oldWhereClause.charAt(rightBrace) == '.') {
					rightBrace++;
				}

				end = rightBrace;
				beginFilterMatcher = filterPattern.matcher(oldWhereClause.substring(end).trim());
			}

			endMatcher = endPattern.matcher(oldWhereClause.substring(end));

			if (endMatcher.lookingAt()) {
				index = end;
				beginFilterMatcher = filterPattern.matcher(oldWhereClause.substring(end));
			} else {
				int depth = 1, length = oldWhereClause.length();
				int position = end;

				newWhereClause = new StringBuffer("");
				while (position < length) {
					if (oldWhereClause.charAt(position) == '{')
						depth++;
					else if (oldWhereClause.charAt(position) == '}')
						depth--;

					position++;

					if (depth == 0)
						break;
				}

				newWhereClause.append(oldWhereClause.substring(0, start));
				newWhereClause.append(oldWhereClause.substring(end, position - 1));
				newWhereClause.append(oldWhereClause.substring(start, end));
				newWhereClause.append(oldWhereClause.substring(position - 1));

				oldWhereClause = newWhereClause.toString();

				beginFilterMatcher = filterPattern.matcher(oldWhereClause.substring(start));
				filterMatcher = filterPattern.matcher(oldWhereClause);
			}
		}

		return oldWhereClause;
	}

	private static boolean isSubQueryMonotonic(String whereClause) {
		Pattern subQueryPattern = Pattern.compile("\\{ *SELECT");
		Matcher subQueryMatcher = subQueryPattern.matcher(whereClause);

		if (subQueryMatcher.find()) {
			whereClause = repalceSubQueryWithVariables(whereClause);

			if (whereClause.equals("-1"))
				return false;

			if (isOptionalMonotonic(whereClause))
				return true;
			else
				return false;
		} else
			try {
				throw new Exception("There are not subquery!");
			} catch (Exception e) {
				// TODO: handle exception
			}

		return true;
	}

	private static boolean isUnionMonotonic(String whereClause) {
		// do not have the optional key world
		if (isUnionWithoutOptional(whereClause)) {
			return true;
		}

		if (!isOptionalUnionFree(whereClause)) {
			return false;
		}

		while (!isUnionNormal(whereClause)) {
			whereClause = rewriteToUnionNormalForm(whereClause);
		}

		if (!isUnionNormal(whereClause))
			try {
				throw new Exception("rewrite error!");
			} catch (Exception e) {
				// TODO: handle exception
			}

		if (unionNormalMonotonic(whereClause))
			return true;
		else
			return false;
	}

	// OPT (P UNION Q)
	private static boolean isOptionalUnionFree(String whereClause) {
		Pattern optionalPattern = Pattern.compile("OPTIONAL *\\{");
		Matcher optionalMatcher = optionalPattern.matcher(whereClause);

		Pattern unionPattern = Pattern.compile("\\} *UNION *\\{");
		Matcher innerUnionMatcher = null;

		int start = 0, end = 0;
		String bagPattern = null;
		while (optionalMatcher.find(end)) {
			start = optionalMatcher.end() - 1;
			end = findTheBraceEnd(whereClause, start);

			bagPattern = whereClause.substring(start, end);

			innerUnionMatcher = unionPattern.matcher(bagPattern);
			if (innerUnionMatcher.find())
				return false;
		}

		return true;
	}

	// do not have optional and filter key word
	private static boolean isUnionWithoutOptional(String whereClause) {
		Pattern optionalPattern = Pattern.compile("OPTIONAL *\\{");
		Matcher optionalMatcher = optionalPattern.matcher(whereClause);

		if (optionalMatcher.find())
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
		if (firstBraceMatcher.lookingAt()) {
			start = firstBraceMatcher.end() - 1;
			end = findTheBraceEnd(whereClause, start);

			bagPattern = whereClause.substring(start, end);

			innerUnionMatcher = unionPattern.matcher(bagPattern);
			if (innerUnionMatcher.find())
				return false;

			beginUnionMatcher = unionPattern.matcher(whereClause.substring(end - 1));
			while (beginUnionMatcher.lookingAt()) {
				if (!unionMatcher.find(end - 1))
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
				if (innerUnionMatcher.find())
					return false;

				beginUnionMatcher = unionPattern.matcher(whereClause.substring(end - 1));
			}
		} else {
			return false;
		}

		Pattern endPattern = Pattern.compile("^ *\\}?$");
		Matcher endMatcher = endPattern.matcher(whereClause.substring(end));

		if (endMatcher.find())
			return true;
		else
			return false;
	}

	// P AND UNION-NORMAL-FORM AND Q
	private static String rewriteToUnionNormalForm(String oldWhereClause) {
		StringBuffer newWhereClause = new StringBuffer("");

		ArrayList<String> unionList = getUnionList(oldWhereClause);

		newWhereClause.append("{");
		if (!unionList.isEmpty()) {
			boolean firstFlag = true;

			for (String union : unionList) {
				if (firstFlag) {
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

		if (isUnionNormal(bagPatterns)) {
			int index = 1, length = bagPatterns.length(), start, end;
			while (index < length) {
				if (bagPatterns.charAt(index) == '{') {
					start = index;
					end = findTheBraceEnd(bagPatterns, start);
					unionNormalFormList.add(bagPatterns.substring(start + 1, end - 1));

					index = end;
				} else {
					index++;
				}
			}

			return unionNormalFormList;
		}

		Pattern unionPattern = Pattern.compile("\\} *UNION *\\{");
		Matcher unionMatcher = unionPattern.matcher(bagPatterns);

		String previousString = null;
		int previousStringEnd = 0;
		if (unionMatcher.find()) {
			int index = unionMatcher.start();
			int last = index + 1;

			index++;
			int depth = 1, length = bagPatterns.length();
			while (index < length) {
				if (depth == 0) {
					depth = 1;
					last = index;
				}

				if (bagPatterns.charAt(index) == '{') {
					depth++;
				} else if (bagPatterns.charAt(index) == '}') {
					depth--;
				}

				index++;
			}

			if (index > length)
				try {
					throw new Exception("It is not the correct union bagpatterns.");
				} catch (Exception e) {
					// TODO: handle exception
					e.printStackTrace();
				}

			previousStringEnd = findTheBracePre(bagPatterns, last - 1);

			if (previousStringEnd < 0)
				try {
					throw new Exception("It is not the correct union bagpatterns.");
				} catch (Exception e) {
					// TODO: handle exception
					e.printStackTrace();
				}

			previousStringEnd++;

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
		if (innerMatcher.find()) {
			unionNormalFormList.addAll(getUnionList(bagPattern));
		} else {
			unionNormalFormList.add(bagPattern.substring(1, bagPattern.length() - 1).trim());
		}

		beginMatcher = unionPattern.matcher(bagPatterns.substring(end - 1));
		unionMatcher = unionMatcher.reset();
		while (beginMatcher.lookingAt()) {
			if (!unionMatcher.find(end - 1))
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
			if (innerMatcher.find()) {
				unionNormalFormList.addAll(getUnionList(bagPattern));
			} else {
				unionNormalFormList.add(bagPattern.substring(1, bagPattern.length() - 1).trim());
			}

			beginMatcher = unionPattern.matcher(bagPatterns.substring(end - 1));
		}

		while (bagPatterns.charAt(end) == ' ')
			end++;

		if (bagPatterns.charAt(end) == '.')
			end++;

		String afterwardString = bagPatterns.substring(end, bagPatterns.length() - 1).trim();

		ArrayList<String> unionList = new ArrayList<String>();
		StringBuffer unionClause = null;
		if (!unionNormalFormList.isEmpty()) {

			for (String union : unionNormalFormList) {
				unionClause = new StringBuffer("");

				if (!previousString.trim().equals(""))
					unionClause.append(" " + previousString + (previousString.endsWith(".") ? " " : " ."));
				unionClause.append(" " + union + (union.endsWith(".") ? " " : " ."));
				if (!afterwardString.trim().equals(""))
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

	private static boolean unionNormalMonotonic(String whereClause) {
		Pattern firstBracePattern = Pattern.compile("\\{ *\\{");
		Matcher firstBraceMatcher = firstBracePattern.matcher(whereClause);

		Pattern unionPattern = Pattern.compile("\\} *UNION *\\{");
		Matcher unionMatcher = unionPattern.matcher(whereClause);

		int start = 0, end = 0;
		String bagPattern = null;
		if (firstBraceMatcher.lookingAt()) {
			start = firstBraceMatcher.end() - 1;
			end = findTheBraceEnd(whereClause, start);

			bagPattern = whereClause.substring(start, end);
			if (!isOptionalUnionFree(bagPattern))
				return false;
			while (unionMatcher.find(end - 1)) {
				start = unionMatcher.end() - 1;
				end = findTheBraceEnd(whereClause, start);

				bagPattern = whereClause.substring(start, end);

				if (!isOptionalUnionFree(bagPattern))
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

		Pattern endPattern = Pattern.compile("^ *\\}?$");
		Matcher endMatcher = endPattern.matcher(whereClause.substring(end));

		if (endMatcher.find())
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
			if (leftBraceMatcher.find(selectVariablesMatcher.end())) {
				left = leftBraceMatcher.end() - 1;
				right = findTheBraceEnd(sourceSparql, left);
			}

			char tempChar, c = '`';
			boolean flag = false;
			StringBuffer sb = new StringBuffer("");
			for (index = left; index < right; index++) {
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

		if (selectVariablesMatcher.find(left)) {
			begin = selectVariablesMatcher.start();
			// returnString = result.substring(0, begin);
			leftBraceMatcher = leftBracePattern.matcher(result.toString());
			while (selectVariablesMatcher.find(begin)) {
				if (leftBraceMatcher.find(selectVariablesMatcher.end())) {
					returnString += result.substring(end, selectVariablesMatcher.start());
					whereIndex = leftBraceMatcher.end() - 1;
					end = findTheBraceEnd(result.toString(), whereIndex);
				}
				selectString = new StringBuffer(
						rewriteSelectSparql(result.substring(selectVariablesMatcher.start(), end), selectVariables));
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
					if (i > 0) {
						if ((string.charAt(i - 1) != ' ') && (string.charAt(i - 1) != '(')
								&& (string.charAt(i - 1) != '{') && (string.charAt(i - 1) != '.')) {
							continue;
						}
					}

					flag = true;
					firstChar = true;
					sb = new StringBuffer("");
				}
			} else {
				if (firstChar) {
					if (!(tempChar >= 'A' && tempChar <= 'Z'))
						flag = false;
					firstChar = false;
					continue;
				}

				if ((tempChar >= 'a' && tempChar <= 'z') || (tempChar >= 'A' && tempChar <= 'Z')
						|| (tempChar >= '0' && tempChar <= '9') || tempChar == '_' || tempChar == '`')
					sb.append(tempChar);
				else {
					flag = false;
					if (!variablesList.contains(sb.toString()))
						variablesList.add(new String(sb));
				}

			}
		}

		return variablesList;
	}

	private static boolean isSafe(String whereClause) {
		if (!whereClause.contains("FILTER"))
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

			if (whereClause.charAt(Qstart) == '(')
				Qend = findTheLittleBraceEnd(whereClause, Qstart);
			else if (whereClause.charAt(Qstart) == '{')
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
				if (whereClause.charAt(Pend) == '{')
					depth++;
				else if (whereClause.charAt(Pend) == '}')
					depth--;

				Pend++;

				if (depth == 0)
					break;
			}

			Pstart = findTheBracePre(whereClause, Pend - 1);
			Pstart++;

			filterAreaList.add(Pstart);
			filterAreaList.add(Pend);

			position = Qstart;
		}

		int filterCount = filterPositionList.size() / 2;
		String Pstring, Qstring;
		ArrayList<String> filterVariablesList = null;
		ArrayList<String> PVariablesList = null;
		int i, j, filterStart;
		for (i = 0; i < filterCount; i++) {
			Qstart = filterPositionList.get(2 * i);
			Qend = filterPositionList.get(2 * i + 1);

			Pstart = filterAreaList.get(2 * i);
			Pend = filterAreaList.get(2 * i + 1);

			Qstring = whereClause.substring(Qstart, Qend);

			Pstring = "";
			Pstring += whereClause.substring(Pstart, Qstart);
			j = i + 1;
			position = Qend;
			while (j < filterCount) {
				filterStart = filterPositionList.get(2 * j);
				if (filterStart < Pend && position < filterStart) {
					Pstring += whereClause.substring(position, filterStart);
					position = filterPositionList.get(2 * j + 1);
					j++;
				} else {
					break;
				}
			}

			Pstring += whereClause.substring(position, Pend);

			filterVariablesList = getVariables(Qstring);
			PVariablesList = getVariables(Pstring);

			// if (filterVariablesList.contains(",")) {
			// System.out.println("heha");
			// }

			if (!PVariablesList.containsAll(filterVariablesList))
				return false;
		}

		return true;
	}

	private static String repalceSubQueryWithVariables(String whereClause) {
		// to do: process subquery
		Pattern subQueryPattern = Pattern.compile("\\{ *SELECT");
		Matcher subQueryMatcher = subQueryPattern.matcher(whereClause);
		int begin = 0;
		while (subQueryMatcher.find(begin)) {

			int subStart = subQueryMatcher.start();
			int subEnd = findTheBraceEnd(whereClause, subStart);
			String subQueryString = null;

			subQueryString = whereClause.substring(subStart, subEnd).trim();

			if (!isMonotonicity(subQueryString.substring(1, subQueryString.length() - 1), true)) {
				return "-1";
			}

			ArrayList<String> variables = getVariables(subQueryString);

			Iterator<String> iterator = variables.iterator();
			String variable = null;
			String replaceString = "";

			if (subStart > 0) {
				replaceString += whereClause.substring(0, subStart);
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

			if (subEnd < whereClause.length()) {
				replaceString += whereClause.substring(subEnd, whereClause.length());
			}

			whereClause = replaceString;

			subQueryMatcher = subQueryPattern.matcher(whereClause);
		}

		return whereClause;
	}

	private static boolean isOptionalMonotonic(String whereClause) {
		if (!whereClause.contains("OPTIONAL"))
			return true;

		if (isOptionalWellDesigned(whereClause)) {
			Pattern optionalPattern = Pattern.compile("OPTIONAL *\\{");
			Matcher optionalMatcher = optionalPattern.matcher(whereClause);

			int Qstart, Qend, Rstart, Rend, Pstart, Pend, index = 0;
			String Pstring, Qstring;
			// process R = P OPT Q
			while (optionalMatcher.find(index)) {
				Qstart = optionalMatcher.end() - 1;
				Qend = findTheBraceEnd(whereClause, Qstart);

				Rend = Qend;
				int depth = 1, length = whereClause.length();
				while (Rend < length) {
					if (whereClause.charAt(Rend) == '{')
						depth++;
					else if (whereClause.charAt(Rend) == '}')
						depth--;

					Rend++;

					if (depth == 0)
						break;
				}

				Rstart = findTheBracePre(whereClause, Rend - 1);
				Rstart++;

				Pstart = Rstart + 1;
				Pend = Qstart;

				Pstring = whereClause.substring(Pstart, Pend);
				Qstring = whereClause.substring(Qstart, Qend);

				ArrayList<String> QVariables = new ArrayList<String>();
				ArrayList<String> PVariables = new ArrayList<String>();

				QVariables = getVariables(Qstring);
				PVariables = getVariables(Pstring);

				String tempString = null;
				Iterator<String> iteratorString = null;
				iteratorString = QVariables.iterator();
				while (iteratorString.hasNext()) {
					tempString = iteratorString.next();
					if (!PVariables.contains(tempString))
						return false;
				}

				index = Qstart;
			}

			return true;
		} else {
			return false;
		}
	}

	private static boolean isOptionalWellDesigned(String whereClause) {
		if (!whereClause.contains("OPTIONAL"))
			return true;

		Pattern optionalPattern = Pattern.compile("OPTIONAL *\\{");
		Matcher optionalMatcher = optionalPattern.matcher(whereClause);

		int Qstart, Qend, Rstart, Rend, Pstart, Pend, index = 0;
		String Pstring, Qstring, Rstring, outString;
		// process R = P OPT Q
		while (optionalMatcher.find(index)) {
			Qstart = optionalMatcher.end() - 1;
			Qend = findTheBraceEnd(whereClause, Qstart);

			Rend = Qend;
			int depth = 1, length = whereClause.length();
			while (Rend < length) {
				if (whereClause.charAt(Rend) == '{')
					depth++;
				else if (whereClause.charAt(Rend) == '}')
					depth--;

				Rend++;

				if (depth == 0)
					break;
			}

			Rstart = findTheBracePre(whereClause, Rend - 1);
			Rstart++;

			Pstart = Rstart + 1;
			Pend = Qstart;

			Pstring = whereClause.substring(Pstart, Pend);
			Qstring = whereClause.substring(Qstart, Qend);
			Rstring = whereClause.substring(Pstart, Qend);

			int before = whereClause.indexOf(Rstring);
			int after = before + Rstring.length();

			outString = "";
			if (before != -1)
				outString += whereClause.substring(0, before);
			if (after < whereClause.length())
				outString += whereClause.substring(after);

			ArrayList<String> QVariables = new ArrayList<String>();
			ArrayList<String> outsideVariables = new ArrayList<String>();
			ArrayList<String> PVariables = new ArrayList<String>();

			QVariables = getVariables(Qstring);
			outsideVariables = getVariables(outString);
			PVariables = getVariables(Pstring);

			String tempString = null;
			Iterator<String> iteratorString = null;
			iteratorString = QVariables.iterator();
			while (iteratorString.hasNext()) {
				tempString = iteratorString.next();
				if (outsideVariables.contains(tempString))
					if (!PVariables.contains(tempString))
						return false;
			}

			index = Qstart;
		}

		return true;
	}
}
