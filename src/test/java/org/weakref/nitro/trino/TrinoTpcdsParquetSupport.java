/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.weakref.nitro.trino;

import io.trino.testing.MaterializedResult;
import org.weakref.nitro.tpcds.TpcdsParquetTables;

import java.nio.file.Path;

public final class TrinoTpcdsParquetSupport
        implements AutoCloseable
{
    private Path currentRootDirectory;
    private String currentSchema;
    private TrinoTpcdsParquetSqlSupport sqlSupport;

    public MaterializedResult query01(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("01");
    }

    public MaterializedResult query02(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("02");
    }

    public MaterializedResult query03(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("03");
    }

    public MaterializedResult query04(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("04");
    }

    public MaterializedResult query05(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("05");
    }

    public MaterializedResult query06(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("06");
    }

    public MaterializedResult query07(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("07");
    }

    public MaterializedResult query08(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("08");
    }

    public MaterializedResult query09(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("09");
    }

    public MaterializedResult query10(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("10");
    }

    public MaterializedResult query11(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("11");
    }

    public MaterializedResult query12(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("12");
    }

    public MaterializedResult query13(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("13");
    }

    public MaterializedResult query14(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("14");
    }

    public MaterializedResult query15(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("15");
    }

    public MaterializedResult query16(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("16");
    }

    public MaterializedResult query17(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("17");
    }

    public MaterializedResult query18(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("18");
    }

    public MaterializedResult query19(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("19");
    }

    public MaterializedResult query20(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("20");
    }

    public MaterializedResult query21(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("21");
    }

    public MaterializedResult query22(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("22");
    }

    public MaterializedResult query23(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("23");
    }

    public MaterializedResult query24(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("24");
    }

    public MaterializedResult query25(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("25");
    }

    public MaterializedResult query26(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("26");
    }

    public MaterializedResult query27(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("27");
    }

    public MaterializedResult query28(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("28");
    }

    public MaterializedResult query29(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("29");
    }

    public MaterializedResult query30(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("30");
    }

    public MaterializedResult query31(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("31");
    }

    public MaterializedResult query32(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("32");
    }

    public MaterializedResult query33(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("33");
    }

    public MaterializedResult query34(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("34");
    }

    public MaterializedResult query35(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("35");
    }

    public MaterializedResult query36(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("36");
    }

    public MaterializedResult query37(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("37");
    }

    public MaterializedResult query38(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("38");
    }

    public MaterializedResult query39(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("39");
    }

    public MaterializedResult query40(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("40");
    }

    public MaterializedResult query41(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("41");
    }

    public MaterializedResult query42(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("42");
    }

    public MaterializedResult query43(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("43");
    }

    public MaterializedResult query44(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("44");
    }

    public MaterializedResult query45(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("45");
    }

    public MaterializedResult query46(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("46");
    }

    public MaterializedResult query47(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("47");
    }

    public MaterializedResult query48(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("48");
    }

    public MaterializedResult query49(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("49");
    }

    public MaterializedResult query50(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("50");
    }

    public MaterializedResult query51(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("51");
    }

    public MaterializedResult query52(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("52");
    }

    public MaterializedResult query53(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("53");
    }

    public MaterializedResult query54(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("54");
    }

    public MaterializedResult query55(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("55");
    }

    public MaterializedResult query56(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("56");
    }

    public MaterializedResult query57(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("57");
    }

    public MaterializedResult query58(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("58");
    }

    public MaterializedResult query59(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("59");
    }

    public MaterializedResult query60(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("60");
    }

    public MaterializedResult query61(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("61");
    }

    public MaterializedResult query62(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("62");
    }

    public MaterializedResult query63(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("63");
    }

    public MaterializedResult query64(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("64");
    }

    public MaterializedResult query65(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("65");
    }

    public MaterializedResult query66(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("66");
    }

    public MaterializedResult query67(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("67");
    }

    public MaterializedResult query68(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("68");
    }

    public MaterializedResult query69(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("69");
    }

    public MaterializedResult query70(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("70");
    }

    public MaterializedResult query71(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("71");
    }

    public MaterializedResult query72(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("72");
    }

    public MaterializedResult query73(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("73");
    }

    public MaterializedResult query74(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("74");
    }

    public MaterializedResult query75(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("75");
    }

    public MaterializedResult query76(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("76");
    }

    public MaterializedResult query77(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("77");
    }

    public MaterializedResult query78(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("78");
    }

    public MaterializedResult query79(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("79");
    }

    public MaterializedResult query80(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("80");
    }

    public MaterializedResult query81(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("81");
    }

    public MaterializedResult query82(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("82");
    }

    public MaterializedResult query83(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("83");
    }

    public MaterializedResult query84(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("84");
    }

    public MaterializedResult query85(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("85");
    }

    public MaterializedResult query86(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("86");
    }

    public MaterializedResult query87(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("87");
    }

    public MaterializedResult query88(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("88");
    }

    public MaterializedResult query89(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("89");
    }

    public MaterializedResult query90(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("90");
    }

    public MaterializedResult query91(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("91");
    }

    public MaterializedResult query92(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("92");
    }

    public MaterializedResult query93(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("93");
    }

    public MaterializedResult query94(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("94");
    }

    public MaterializedResult query95(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("95");
    }

    public MaterializedResult query96(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("96");
    }

    public MaterializedResult query97(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("97");
    }

    public MaterializedResult query98(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("98");
    }

    public MaterializedResult query99(TpcdsParquetTables tables)
    {
        return support(tables).executeBenchmarkQuery("99");
    }

    @Override
    public void close()
    {
        if (sqlSupport != null) {
            sqlSupport.close();
            sqlSupport = null;
        }
        currentRootDirectory = null;
        currentSchema = null;
    }

    private TrinoTpcdsParquetSqlSupport support(TpcdsParquetTables tables)
    {
        Path requestedRootDirectory = tables.rootDirectory().toAbsolutePath().normalize();
        String requestedSchema = tables.schema();
        if (sqlSupport == null || !requestedRootDirectory.equals(currentRootDirectory) || !requestedSchema.equals(currentSchema)) {
            close();
            sqlSupport = new TrinoTpcdsParquetSqlSupport(tables);
            currentRootDirectory = requestedRootDirectory;
            currentSchema = requestedSchema;
        }
        return sqlSupport;
    }
}
