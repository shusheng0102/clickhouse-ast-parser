package com.clickhouse.parser;

import com.clickhouse.parser.ast.*;
import com.clickhouse.parser.ast.expr.*;

import java.util.List;

public class AstVisitor<T> {

    public T visit(INode astNode) {
        return astNode.accept(this);
    }

    public T visitDataClause(DataClause dataClause) {
        if (null != dataClause.getIdentifier()) {
            visit(dataClause.getIdentifier());
        }
        if (null != dataClause.getSelectUnionQuery()) {
            visit(dataClause.getSelectUnionQuery());
        }
        return null;
    }

    public T visitSelectUnionQuery(SelectUnionQuery selectUnionQuery) {
        for (SelectStatement selectStatement : selectUnionQuery.getStatements()) {
            visitSelectStatement(selectStatement);
        }
        return null;
    }

    public T visitSelectStatement(SelectStatement selectStatement) {
        if (null != selectStatement.getWithClause()) {
            visitWithClause(selectStatement.getWithClause());
        }
        if (null != selectStatement.getExprs()) {
            visitColumnExprList(selectStatement.getExprs());
        }
        if (null != selectStatement.getFromClause()) {
            visitFromClause(selectStatement.getFromClause());
        }
        if (null != selectStatement.getArrayJoinClause()) {
            visitArrayJoinClause(selectStatement.getArrayJoinClause());
        }
        if (null != selectStatement.getPrewhereClause()) {
            visitPrewhereClause(selectStatement.getPrewhereClause());
        }
        if (null != selectStatement.getWhereClause()) {
            visitWhereClause(selectStatement.getWhereClause());
        }
        if (null != selectStatement.getGroupByClause()) {
            visitGroupByClause(selectStatement.getGroupByClause());
        }
        if (null != selectStatement.getHavingClause()) {
            visitHavingClause(selectStatement.getHavingClause());
        }
        if (null != selectStatement.getOrderByClause()) {
            visitOrderByClause(selectStatement.getOrderByClause());
        }
        if (null != selectStatement.getLimitByClause()) {
            visitLimitByClause(selectStatement.getLimitByClause());
        }
        if (null != selectStatement.getLimitClause()) {
            visitLimitClause(selectStatement.getLimitClause());
        }
        if (null != selectStatement.getSettingsClause()) {
            visitSettingsClause(selectStatement.getSettingsClause());
        }
        return null;
    }

    public T visitWithClause(WithClause withClause) {
        visitColumnExprList(withClause.getWithExpr());
        return null;
    }

    public T visitFromClause(FromClause fromClause) {
        if (null != fromClause.getExpr()) {
            visitJoinExpr(fromClause.getExpr());
        }
        return null;
    }

    public T visitJoinExpr(JoinExpr joinExpr) {
        if (null != joinExpr.getTableExpr()) {
            visitTableExpr(joinExpr.getTableExpr());
        }
        if (null != joinExpr.getSampleClause()) {
            visitSampleClause(joinExpr.getSampleClause());
        }
        if (null != joinExpr.getLeftExpr()) {
            visitJoinExpr(joinExpr.getLeftExpr());
        }
        if (null != joinExpr.getRightExpr()) {
            visitJoinExpr(joinExpr.getRightExpr());
        }
        if (null != joinExpr.getJoinConstraintClause()) {
            visitJoinConstraintClause(joinExpr.getJoinConstraintClause());
        }
        return null;
    }

    public T visitTableExpr(TableExpr tableExpr) {
        if (null != tableExpr.getExpr()) {
            visitTableExpr(tableExpr.getExpr());
        }
        if (null != tableExpr.getAlias()) {
            visitIdentifier(tableExpr.getAlias());
        }
        if (null != tableExpr.getFunction()) {
            visitTableFunctionExpr(tableExpr.getFunction());
        }
        if (null != tableExpr.getIdentifier()) {
            visitTableIdentifier(tableExpr.getIdentifier());
        }
        if (null != tableExpr.getSubQuery()) {
            visitSelectUnionQuery(tableExpr.getSubQuery());
        }
        return null;
    }

    public T visitIdentifier(Identifier identifier) {
        return null;
    }

    public T visitTableFunctionExpr(TableFunctionExpr function) {
        if (null != function.getName()) {
            visitIdentifier(function.getName());
        }
        if (null != function.getArgs()) {
            visitTableArgExprList(function.getArgs());
        }
        return null;
    }

    public T visitTableArgExprList(List<TableArgExpr> args) {
        for (TableArgExpr arg : args) {
            visitTableArgExpr(arg);
        }
        return null;
    }

    public T visitTableArgExpr(TableArgExpr arg) {
        if (null != arg.getLiteral()) {
            visitLiteral(arg.getLiteral());
        }
        if (null != arg.getFunctionExpr()) {
            visitTableFunctionExpr(arg.getFunctionExpr());
        }
        if (null != arg.getIdentifier()) {
            visitTableIdentifier(arg.getIdentifier());
        }
        return null;
    }

    public T visitLiteral(Literal literal) {
        if (literal instanceof NumberLiteral) {
            visitNumberLiteral((NumberLiteral) literal);
        }
        if (literal instanceof StringLiteral) {
            visitStringLiteral((StringLiteral) literal);
        }
        return null;
    }

    public T visitTableIdentifier(TableIdentifier tableIdentifier) {
        if (null != tableIdentifier.getDatabase()) {
            visitIdentifier(tableIdentifier.getDatabase());
        }
        return null;
    }

    public T visitSampleClause(SampleClause sampleClause) {
        if (null != sampleClause.getRatio()) {
            visitRatioExpr(sampleClause.getRatio());
        }
        if (null != sampleClause.getOffset()) {
            visitRatioExpr(sampleClause.getOffset());
        }
        return null;
    }

    public T visitRatioExpr(RatioExpr ratioExpr) {
        if (null != ratioExpr.getNumerator()) {
            visitNumberLiteral(ratioExpr.getNumerator());
        }
        if (null != ratioExpr.getDenominator()) {
            visitNumberLiteral(ratioExpr.getDenominator());
        }
        return null;
    }

    public T visitNumberLiteral(NumberLiteral numberLiteral) {
        return null;
    }

    public T visitJoinConstraintClause(JoinConstraintClause joinConstraintClause) {
        if (null != joinConstraintClause.getExprs()) {
            visitColumnExprList(joinConstraintClause.getExprs());
        }
        return null;
    }


    public T visitArrayJoinClause(ArrayJoinClause arrayJoinClause) {
        if (null != arrayJoinClause.getExprs()) {
            visitColumnExprList(arrayJoinClause.getExprs());
        }
        return null;
    }

    public T visitPrewhereClause(PrewhereClause prewhereClause) {
        if (null != prewhereClause.getPrewhereExpr()) {
            visitColumnExpr(prewhereClause.getPrewhereExpr());
        }
        return null;
    }

    public T visitWhereClause(WhereClause whereClause) {
        if (null != whereClause.getWhereExpr()) {
            visitColumnExpr(whereClause.getWhereExpr());
        }
        return null;
    }

    public T visitGroupByClause(GroupByClause groupByClause) {
        if (null != groupByClause.getGroupByExprs()) {
            visitColumnExprList(groupByClause.getGroupByExprs());
        }
        return null;
    }

    public T visitHavingClause(HavingClause havingClause) {
        if (null != havingClause.getHavingExpr()) {
            visitColumnExpr(havingClause.getHavingExpr());
        }
        return null;
    }

    public T visitOrderByClause(OrderByClause orderByClause) {
        if (null != orderByClause.getOrderExprs()) {
            for (OrderExpr orderExpr : orderByClause.getOrderExprs()) {
                visitOrderExpr(orderExpr);
            }
        }
        return null;
    }

    public T visitOrderExpr(OrderExpr orderExpr) {
        if (null != orderExpr.getExpr()) {
            visitColumnExpr(orderExpr.getExpr());
        }
        if (null != orderExpr.getCollate()) {
            visitStringLiteral(orderExpr.getCollate());
        }
        return null;
    }

    public T visitStringLiteral(StringLiteral stringLiteral) {
        return null;
    }

    public T visitLimitByClause(LimitByClause limitByClause) {
        if (null != limitByClause.getLimit()) {
            visitLimitExpr(limitByClause.getLimit());
        }
        if (null != limitByClause.getExprs()) {
            visitColumnExprList(limitByClause.getExprs());
        }
        return null;
    }

    public T visitLimitExpr(LimitExpr limitExpr) {
        if (null != limitExpr.getLimit()) {
            visitColumnExpr(limitExpr.getLimit());
        }
        if (null != limitExpr.getOffset()) {
            visitColumnExpr(limitExpr.getOffset());
        }
        return null;
    }

    public T visitLimitClause(LimitClause limitClause) {
        if (null != limitClause.getLimitExpr()) {
            visitLimitExpr(limitClause.getLimitExpr());
        }
        return null;
    }

    public T visitSettingsClause(SettingsClause settingsClause) {
        if (null != settingsClause.getSettingExprs()) {
            for (SettingExpr settingExpr : settingsClause.getSettingExprs()) {
                visitSettingExpr(settingExpr);
            }
        }
        return null;
    }

    public T visitSettingExpr(SettingExpr settingExpr) {
        if (null != settingExpr.getName()) {
            visitIdentifier(settingExpr.getName());
        }
        if (null != settingExpr.getValue()) {
            visitLiteral(settingExpr.getValue());
        }
        return null;
    }

    public T visitSelectColumnExprList(List<ColumnExpr> exprs) {
        for (ColumnExpr expr : exprs) {
            visitColumnExpr(expr);
        }
        return null;
    }

    public T visitColumnExprList(List<ColumnExpr> exprs) {
        for (ColumnExpr expr : exprs) {
            visitColumnExpr(expr);
        }
        return null;
    }

    public T visitAsteriskColumnExpr(AsteriskColumnExpr expr) {
        return null;
    }

    public T visitColumnExpr(ColumnExpr expr) {
        if (expr instanceof AsteriskColumnExpr) {
            return visitAsteriskColumnExpr((AsteriskColumnExpr) expr);
        }
        if (expr instanceof AliasColumnExpr) {
            return visitAliasColumnExpr((AliasColumnExpr) expr);
        }
        if (expr instanceof FunctionColumnExpr) {
            return visitFunctionColumnExpr(expr);
        }
        if (expr instanceof SubqueryColumnExpr) {
            return visitSubqueryColumnExpr(expr);
        }
        if (expr instanceof IdentifierColumnExpr) {
            return visitIdentifierColumnExpr(expr);
        }
        if (expr instanceof LiteralColumnExpr) {
            return visitLiteralColumnExpr(expr);
        }
        return null;
    }

    public T visitLiteralColumnExpr(ColumnExpr expr) {
        if (null != expr && expr instanceof LiteralColumnExpr) {
            LiteralColumnExpr literalColumnExpr = (LiteralColumnExpr) expr;
        }
        return null;
    }

    public T visitIdentifierColumnExpr(ColumnExpr expr) {
        if (null != expr && expr instanceof IdentifierColumnExpr) {
            IdentifierColumnExpr identifierColumnExpr = (IdentifierColumnExpr) expr;
            return visitIdentifier(identifierColumnExpr.getIdentifier());
        }
        return null;
    }

    public T visitAliasColumnExpr(AliasColumnExpr expr) {
        if (null != expr.getExpr()) {
            visitColumnExpr(expr.getExpr());
        }
        if (null != expr.getAlias()) {
            visitIdentifier(expr.getAlias());
        }
        return null;
    }

    public T visitFunctionColumnExpr(ColumnExpr expr) {
        if (null != expr && expr instanceof FunctionColumnExpr) {
            FunctionColumnExpr functionColumnExpr = (FunctionColumnExpr) expr;
            if (null != functionColumnExpr.getName()) {
                visitIdentifier(functionColumnExpr.getName());
            }
            if (null != functionColumnExpr.getParams()) {
                visitColumnExprList(functionColumnExpr.getParams());
            }
            if (null != functionColumnExpr.getArgs()) {
                visitColumnExprList(functionColumnExpr.getArgs());
            }
        }
        return null;
    }

    public T visitSubqueryColumnExpr(ColumnExpr expr) {
        if (null != expr && expr instanceof SubqueryColumnExpr) {
            SubqueryColumnExpr subqueryColumnExpr = (SubqueryColumnExpr) expr;
        }
        return null;
    }

}
