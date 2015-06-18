/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.drill.exec.store.mpjdbc;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.drill.common.expression.BooleanOperator;
import org.apache.drill.common.expression.CastExpression;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.ValueExpressions.BooleanExpression;
import org.apache.drill.common.expression.ValueExpressions.DateExpression;
import org.apache.drill.common.expression.ValueExpressions.Decimal18Expression;
import org.apache.drill.common.expression.ValueExpressions.Decimal28Expression;
import org.apache.drill.common.expression.ValueExpressions.Decimal38Expression;
import org.apache.drill.common.expression.ValueExpressions.Decimal9Expression;
import org.apache.drill.common.expression.ValueExpressions.DoubleExpression;
import org.apache.drill.common.expression.ValueExpressions.FloatExpression;
import org.apache.drill.common.expression.ValueExpressions.IntExpression;
import org.apache.drill.common.expression.ValueExpressions.QuotedString;
import org.apache.drill.common.expression.ValueExpressions.TimeExpression;
import org.apache.drill.common.expression.ValueExpressions.TimeStampExpression;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;
import org.apache.drill.common.types.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.store.mpjdbc.MPJdbcScanSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

public class MPJdbcFilterBuilder extends AbstractExprVisitor<MPJdbcScanSpec, Void, RuntimeException> {
    static final Logger logger = LoggerFactory
            .getLogger(MPJdbcFilterBuilder.class);
    final MPJdbcGroupScan groupScan;
    final LogicalExpression le;
    private boolean allExpressionsConverted = true;

    public MPJdbcFilterBuilder(MPJdbcGroupScan groupScan,
            LogicalExpression conditionExp) {
        this.groupScan = groupScan;
        this.le = conditionExp;
        allExpressionsConverted = true;
    }

    public MPJdbcScanSpec parseTree() {
        MPJdbcScanSpec parsedSpec = le.accept(this, null);
        if (parsedSpec != null) {
            parsedSpec = mergeScanSpecs("booleanAnd",
                    this.groupScan.getScanSpec(), parsedSpec);
        }
        return parsedSpec;
    }

    private MPJdbcScanSpec mergeScanSpecs(String functionName,
            MPJdbcScanSpec leftScanSpec, MPJdbcScanSpec rightScanSpec) {
        List<String> newFilter = new ArrayList<String>();
        switch (functionName) {
        case "booleanAnd":
            if (leftScanSpec.getFilters() != null
                    && rightScanSpec.getFilters() != null) {
                newFilter.addAll(leftScanSpec.getFilters());
                newFilter.add(" AND ");
                newFilter.addAll(rightScanSpec.getFilters());
            } else if (leftScanSpec.getFilters() != null) {
                newFilter.addAll(leftScanSpec.getFilters());
            } else {
                newFilter.addAll(rightScanSpec.getFilters());
            }
            break;
        case "booleanOr":
            if (leftScanSpec.getFilters() != null
                    && rightScanSpec.getFilters() != null) {
                newFilter = leftScanSpec.getFilters();
                newFilter.add(" OR ");
                newFilter.addAll(rightScanSpec.getFilters());
            } else if (leftScanSpec.getFilters() != null) {
                newFilter = leftScanSpec.getFilters();
            } else {
                newFilter = rightScanSpec.getFilters();
            }
            break;

        default:
            allExpressionsConverted = false;
            break;
        }

        MPJdbcScanSpec mp = new MPJdbcScanSpec(this.groupScan.getScanSpec()
                .getDatabase(), this.groupScan.getScanSpec().getTable(),
                newFilter, this.groupScan.getScanSpec().getColumns());
        return mp;
    }

    public boolean isAllExpressionsConverted() {
        return allExpressionsConverted;
    }

    @Override
    public MPJdbcScanSpec visitUnknown(LogicalExpression e, Void value)
            throws RuntimeException {
        MPJdbcScanSpec spec = null;
        String valueStr = null;
        StringBuilder strBuilder = null;
        LogicalExpression Expr = null;

        if (e instanceof QuotedString) {
            QuotedString val = (QuotedString) e;
            valueStr = "'" + val.value + "'";
        } else if (e instanceof BooleanExpression) {
            BooleanExpression bval = (BooleanExpression) e;
            valueStr = bval.getBoolean() == true ? "true" : "false";
        } else if (e instanceof IntExpression) {
            IntExpression ival = (IntExpression) e;
            valueStr = new Integer(ival.getInt()).toString();
        } else if (e instanceof DateExpression) {
            DateExpression dval = (DateExpression) e;
            long dt = dval.getDate();
            java.sql.Date d = new java.sql.Date(dt);
            valueStr = d.toString();
        } else if (e instanceof TimeExpression) {
            TimeExpression tval = (TimeExpression) e;
            int tm = tval.getTime();
            java.sql.Time t = new java.sql.Time(tm);
            valueStr = t.toString();
        } else if (e instanceof TimeStampExpression) {
            TimeStampExpression tsval = (TimeStampExpression) e;
            long ts = tsval.getTimeStamp();
            java.sql.Timestamp ts_val = new java.sql.Timestamp(ts);
            valueStr = ts_val.toString();
        } else if (e instanceof DoubleExpression) {
            DoubleExpression dval = (DoubleExpression) e;
            valueStr = new Double(dval.getDouble()).toString();
        } else if (e instanceof FloatExpression) {
            FloatExpression fval = (FloatExpression) e;
            valueStr = new Float(fval.getFloat()).toString();
        } else if (e instanceof Decimal9Expression) {
            Decimal9Expression d9val = (Decimal9Expression) e;
            spec = d9val.accept(this, null);
            valueStr = spec.getFilters().get(0);
        } else if (e instanceof Decimal18Expression) {
            Decimal18Expression d18val = (Decimal18Expression) e;
            spec = d18val.accept(this, null);
            valueStr = spec.getFilters().get(0);
        } else if (e instanceof Decimal28Expression) {
            Decimal28Expression d28val = (Decimal28Expression) e;
            spec = d28val.accept(this, null);
            valueStr = spec.getFilters().get(0);
        } else if (e instanceof Decimal38Expression) {
            Decimal38Expression d38val = (Decimal38Expression) e;
            spec = d38val.accept(this, null);
            valueStr = spec.getFilters().get(0);
        } else if (e instanceof CastExpression) {
            CastExpression cExpr = (CastExpression) e;
            Expr = cExpr.getInput();
            strBuilder = new StringBuilder();
            strBuilder = this.genericFunctionCall(Expr, strBuilder);
            valueStr = strBuilder.toString();
            MajorType type = cExpr.getMajorType();
            int JdbcType = Types.getJdbcType(type);
            int width = type.getWidth();
            int precision = type.getPrecision();
            int scale = type.getScale();

            switch (JdbcType) {
            case java.sql.Types.BOOLEAN:
                valueStr += " as boolean";
                break;

            case java.sql.Types.BINARY:
                if(type.hasWidth()) {
                     valueStr += " as varbinary(" + new Integer(width).toString() + ")";
                }
                else {
                    valueStr += " as varbinary";
                }
                break;

            case java.sql.Types.NCHAR:
                if(type.hasWidth()) {
                    valueStr += " as nchar(" + new Integer(width).toString() + ")";
                }
                else {
                    valueStr += " as nchar";
                }
                break;

            case java.sql.Types.FLOAT:
                valueStr += " as float";
                break;

            case java.sql.Types.DOUBLE:
                valueStr += " as double";
                break;

            case java.sql.Types.INTEGER:
                valueStr += " as integer";
                break;

            case java.sql.Types.STRUCT:
                break;

            case java.sql.Types.DECIMAL:
                if(type.hasScale() && type.hasPrecision()) {
                    valueStr += " as decimal("+ new Integer(scale).toString() +"," + new Integer(precision).toString() + ")";
                }
                else if(type.hasScale() && type.hasPrecision() == false) {
                    valueStr += " as decimal("+ new Integer(scale).toString() + ")";
                }
                else {
                    valueStr += " as decimal";
                }
                break;

            case java.sql.Types.TIME:
                valueStr += " as time";
                break;

            case java.sql.Types.TIMESTAMP:
                valueStr += " as timestamp";
                break;

            case java.sql.Types.DATE:
                valueStr += " as date";
                break;

            case java.sql.Types.TINYINT:
                valueStr += " as tinyint";
                break;

            case java.sql.Types.SMALLINT:
                valueStr += " as smallint";
                break;

            case java.sql.Types.BIGINT:
                valueStr += " as bigint";
                break;

            case java.sql.Types.NVARCHAR:
                if(type.hasWidth()) {
                    valueStr += "as nvarchar("+ new Integer(width).toString() + ")";
                }
                else {
                    valueStr += "as nvarchar";
                }
                break;

            case java.sql.Types.VARBINARY:

                if(type.hasWidth()) {
                    valueStr += " as varbinary("+ new Integer(width).toString() + ")";
                }
                else {
                    valueStr += " as varbinary";
                }
                break;

            case java.sql.Types.VARCHAR:
                String dbName;
                try {
                    dbName = groupScan.getClient().getConnection().getMetaData().getDatabaseProductName();
                    if(dbName.toUpperCase().contains("MYSQL") == false) {
                    if(type.hasWidth()) {
                        valueStr += " as varchar("+ new Integer(width).toString() + ")";
                    }
                    else {
                        valueStr += " as varchar";
                    }
                    } else {
                        if(type.hasWidth()) {
                            valueStr += " as char("+ new Integer(width).toString() + ")";
                        }
                        else {
                            valueStr += " as char";
                        }
                    }
                } catch (SQLException e1) {
                    // TODO Auto-generated catch block
                    e1.printStackTrace();
                }
                break;
                default:
                    allExpressionsConverted = false;
                break;

            }
            valueStr = "CAST(" + valueStr + ")";
        }


        if(spec == null) {
            spec = new MPJdbcScanSpec(this.groupScan.getScanSpec().getDatabase(), this.groupScan.getScanSpec().getTable(),this.groupScan.getScanSpec().getColumns());
        }
        spec.setFilterString(valueStr);
        return spec;

    }

    @Override
    public MPJdbcScanSpec visitBooleanOperator(BooleanOperator op, Void value) {
        List<LogicalExpression> args = op.args;
        MPJdbcScanSpec nodeScanSpec = null;
        String functionName = op.getName();
        for (int i = 0; i < args.size(); ++i) {
            switch (functionName) {
            case "booleanAnd":
            case "booleanOr":
                if (nodeScanSpec == null) {
                    nodeScanSpec = args.get(i).accept(this, null);
                } else {
                    MPJdbcScanSpec scanSpec = args.get(i).accept(this, null);
                    if (scanSpec != null) {
                        nodeScanSpec = mergeScanSpecs(functionName,
                                nodeScanSpec, scanSpec);
                    } else {
                        allExpressionsConverted = false;
                    }
                }
                break;
                default:
                    allExpressionsConverted = false;
                break;
            }
        }
        return nodeScanSpec;
    }

    public StringBuilder genericFunctionCall(LogicalExpression nameVal,
            StringBuilder filterStatement) {

        List<String> filterString = null;
        Iterator<String> filtStrIter = null;
        MPJdbcScanSpec spec = null;

        if (nameVal instanceof FieldReference) {
            FieldReference NameFR = (FieldReference) nameVal;
            filterStatement.append(NameFR.getAsUnescapedPath());
        } else {
            spec = nameVal.accept(this, null);
            filterString = spec.getFilters();
            filtStrIter = filterString.iterator();
            while (filtStrIter.hasNext()) {
                filterStatement.append(filtStrIter.next());
            }
        }
        return filterStatement;
    }

    @Override
    public MPJdbcScanSpec visitFunctionCall(FunctionCall call, Void value)
            throws RuntimeException {
        MPJdbcScanSpec nodeScanSpec = null;
        String functionName = call.getName();
        String val = null;
        LogicalExpression valueVal = null;
        ImmutableList<LogicalExpression> args = call.args;
        LogicalExpression nameVal = null;
        StringBuilder filterStatement = new StringBuilder();

        if(args.size() > 0) {
          nameVal = call.args.get(0);

            if (args.size() > 1) {
                valueVal = call.args.get(1);
            }
            filterStatement = this.genericFunctionCall(nameVal, filterStatement);
        }
        switch (functionName) {

        case "equal":
            filterStatement.append(" = ");
            filterStatement = this.genericFunctionCall(valueVal,
                    filterStatement);
            break;

        case "not_equal":
            filterStatement.append(" <> ");
            filterStatement = this.genericFunctionCall(valueVal,
                    filterStatement);
            break;

        case "like":
            filterStatement.append(" ");
            filterStatement.append(functionName);
            filterStatement = this.genericFunctionCall(valueVal,
                    filterStatement);
            break;

        case "greater_than_or_equal_to":
            filterStatement.append(" >= ");
            filterStatement = this.genericFunctionCall(valueVal,
                    filterStatement);
            break;

        case "greater_than":
            filterStatement.append(" > ");
            filterStatement = this.genericFunctionCall(valueVal,
                    filterStatement);
            break;

        case "less_than_or_equal_to":
            filterStatement.append(" <= ");
            filterStatement = this.genericFunctionCall(valueVal,
                    filterStatement);
            break;

        case "less_than":
            filterStatement.append(" < ");
            filterStatement = this.genericFunctionCall(valueVal,
                    filterStatement);
            break;

        case "isnull":
        case "isNull":
        case "is null":
            filterStatement.append(" IS NULL ");
            break;

        case "isnotnull":
        case "isNotNull":
        case "is not null":
            filterStatement.append(" IS NOT NULL ");
            break;

        case "not":
        case "exists":
        case "upper":
        case "lower":
            filterStatement.insert(0, "(");
            filterStatement.insert(0, functionName);
            filterStatement.append(")");
            break;

        case "substring":
            filterStatement.insert(0, "(");
            filterStatement.insert(0, functionName);
            filterStatement.append(" FROM ");
            filterStatement = this.genericFunctionCall(valueVal,
                    filterStatement);
            filterStatement.append(" FOR ");
            filterStatement = this.genericFunctionCall(call.args.get(2),
                    filterStatement);
            filterStatement.append(")");
            break;

        default:
            allExpressionsConverted = false;
            break;
        }
        nodeScanSpec = new MPJdbcScanSpec(this.groupScan.getScanSpec()
                .getDatabase(), this.groupScan.getScanSpec().getTable(),
                this.groupScan.getScanSpec().getColumns());

        nodeScanSpec.setFilterString(filterStatement.toString());

        return nodeScanSpec;
    }

}
