import io.dbmaster.testng.BaseToolTestNGCase;

import static org.testng.Assert.assertTrue;
import org.testng.annotations.Test

import com.branegy.tools.api.ExportType;


public class JobListIT extends BaseToolTestNGCase {

    @Test
    public void test() {
        def parameters = [ : ]
        String result = tools.toolExecutor("jobs-list", parameters).execute()
        assertTrue(result.contains("server_name"), "Unexpected search results ${result}");
    }
}
