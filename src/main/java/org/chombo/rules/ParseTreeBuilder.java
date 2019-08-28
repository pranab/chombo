/*
 * chombo: Hadoop Map Reduce utility
 * Author: Pranab Ghosh
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.chombo.rules;

import java.util.ArrayList;
import java.util.List;


/**
 * @author pranab
 *
 */
public class ParseTreeBuilder {
	private static final String TOKEN_SEP = "\\s+";
	private Expression root;
	private Expression current;
	
	public Expression getRoot() {
		return root;
	}

	/**
	 * @param exprStr
	 * @return
	 */
	public  Expression buildParseTree(String exprStr) {
		root = new Expression(null, null, "root");
		current = root;
		
		//iterate through each token and build parse tree
		String[] tokens = exprStr.split(TOKEN_SEP);
		for (int i = 0; i <  tokens.length;) {
			String token = tokens[i];
			Expression thisExpr = create(null, token);
			
			if (thisExpr instanceof ParenthesisOperator) {
				StringBuilder stBld = new StringBuilder();
				int j = i + 1;
				for ( ; !tokens[j].equals(")"); ++j) {
					stBld.append(tokens[j]).append(" ");
				}
				ParseTreeBuilder builder = new ParseTreeBuilder();
				Expression root = builder.buildParseTree(stBld.toString());
				thisExpr.addChild(root);
				addNode(thisExpr);
				i = j + 1;
			} else if (thisExpr instanceof FunctionOperator) {
				List<String> args = new ArrayList<String>();
				int j = i + 1;
				for ( ; !tokens[j].equals(")"); ++j) {
					//strip coma
					String tok = tokens[j].substring(0, tokens[j].length()-1);
					args.add(tok);
				}
				String name = token.substring(0, token.length()-1);
				((FunctionOperator)thisExpr).withName(name).withArgs(args);
				addNode(thisExpr);
				i = j + 1;
			} else {
				addNode(thisExpr);
				++i;
			}
		}
		
		return root;
	}
	
	/**
	 * @param expr
	 */
	private void addNode(Expression expr) {
		int prec = expr.getPrecedence();
		if (prec > current.getPrecedence()) {
			//higher precedence, insert below
			expr.setParent(current);
			current.addChild(expr);
		} else {
			//lower precedence,walk upwards until a node is found with lower precedence and insert below
			Expression next = current;
			Expression prev = null;
			for ( ; next != root && prec <= next.getPrecedence(); prev = next, next = next.getParent()) {}
			if (null != prev) {
				if (prev.getPrecedence() > prec) {
					//insert between next and prev
					expr.setParent(next);
					next.addChild(expr);
					current = expr;
				} else {
					//same precedence as prev
					current = prev;
				}
				next.removeChild(prev);
				prev.setParent(expr);
				expr.addChild(prev);
			} else {
				expr.setParent(next);
				next.addChild(expr);
				current = expr;
			}
		}
	}
	
	/**
	 * @param parent
	 * @param token
	 * @return
	 */
	private Expression create(Expression parent, String token) {
		Expression expr = null;
		
		if (token.equals(Operator.IF_OP)) {
			expr = new IfOperator(root, parent, token);
		} else if (token.equals(Operator.THEN_OP)) {
			expr = new ThenOperator(root, parent, token);
		} else if (token.equals(Operator.OR_OP)) {
			expr = new OrOperator(root, parent, token);
		} else if (token.equals(Operator.AND_OP)) {
			expr = new AndOperator(root, parent, token);
		} else if (token.equals(Operator.EQUAL_TO_OP) || token.equals(Operator.EQUAL_TO_STRING_OP) || 
			token.equals(Operator.LESS_THAN_OP) || token.equals(Operator.LESS_THAN_EQUAL_TO_OP) ||
			token.equals(Operator.GREATER_THAN_OP)	|| token.equals(Operator.GREATER_THAN_EQUAL_TO_OP)) {
			expr = new RelationalOperator(root, parent, token);
		} else if (token.equals(Operator.PLUS_OP) || token.equals(Operator.MINUS_OP)) {
			expr = new AdditiveOperator(root, parent, token);
		} else if (token.equals(Operator.MULTIPLY_OP) || token.equals(Operator.DIVIDE_OP)) {
			expr = new MultiplicativeOperator(root, parent, token);
		} else if (token.equals(Operator.PARENTHESIS_OP)) {
			expr = new ParenthesisOperator(root, parent, token);
		} else if (token.length() > 1 && token.endsWith("(")) {
			expr = new FunctionOperator(root, parent, token.substring(0, token.length()-1));
		} else if (token.startsWith("$")) {
			expr = new VariableTerm(root, parent, token.substring(1));
		} else {
			expr = new LiteralTerm(root, parent, token);
		} 
		
		return expr;
	}
}
