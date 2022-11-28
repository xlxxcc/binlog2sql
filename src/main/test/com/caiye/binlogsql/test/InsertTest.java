package com.caiye.binlogsql.test;

import com.caiye.binlogsql.SqlTest;
import com.caiye.binlogsql.TableTestPairVo;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.RepeatedTest;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.RepeatedTest.CURRENT_REPETITION_PLACEHOLDER;
import static org.junit.jupiter.api.RepeatedTest.DISPLAY_NAME_PLACEHOLDER;

@SuppressWarnings({"AlibabaClassMustHaveAuthor", "WeakerAccess"})
@Slf4j
public class InsertTest extends SqlTest {

    public TableTestPairVo COMMON_TABLE_PAIR = new TableTestPairVo("flashback_test_common", "test_flashback_test_common");

    @RepeatedTest(value = 3, name = DISPLAY_NAME_PLACEHOLDER + "  " + CURRENT_REPETITION_PLACEHOLDER)
    public void insertTest1() throws Exception {
        test("insertTestMultiply.sql", COMMON_TABLE_PAIR);
    }

    @RepeatedTest(value = 3, name = DISPLAY_NAME_PLACEHOLDER + "  " + CURRENT_REPETITION_PLACEHOLDER)
    public void insertTest2() throws Exception {
        test("insertTestMultiply.sql", COMMON_TABLE_PAIR);
    }

    @Override
    protected List<TableTestPairVo> getTableTestPairVo() {
        return Collections.singletonList(COMMON_TABLE_PAIR);
    }
}

