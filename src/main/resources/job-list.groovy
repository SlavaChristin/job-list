import com.branegy.service.connection.api.ConnectionService
import io.dbmaster.api.groovy.DbmTools
import groovy.sql.Sql

def QUERY = """
        WITH jobs 
        AS (
            SELECT
                @@SERVERNAME    server_name,                
                j.job_id        job_id,
                j.name          job_name,
                j.description   job_description,
                j.date_created  date_created,
                j.date_modified date_modified,
                CAST(j.enabled as bit) job_enabled,
                CAST(CASE WHEN ja.run_requested_date IS NOT NULL AND ja.stop_execution_date IS NULL 
                         THEN 1
                         ELSE 0 
                    END AS bit) running,
                c.name AS category,
                COALESCE(ja.start_execution_date, msdb.dbo.agent_datetime(jh.run_date, jh.run_time)) last_start_date,
                ja.next_scheduled_run_date next_run_date
            FROM msdb.dbo.sysjobs j
            JOIN msdb.dbo.syscategories c ON j.category_id = c.category_id
            OUTER APPLY (SELECT TOP 1 *
                         FROM msdb.dbo.sysjobactivity ja
                         WHERE j.job_id = ja.job_id
                         ORDER BY ja.run_requested_date DESC) ja
            LEFT JOIN msdb.dbo.sysjobhistory jh  ON j.job_id = jh.job_id AND ja.job_history_id = jh.instance_id
            LEFT JOIN msdb.dbo.sysjobsteps s ON ja.job_id = s.job_id AND ja.last_executed_step_id = s.step_id
        )
        SELECT
            *,
            DATEDIFF(DAY, date_modified,   GETDATE())  as modified_days_ago,
            DATEDIFF(DAY, last_start_date, GETDATE())  as run_days_ago,
            'exec msdb.dbo.sp_delete_job @job_id =''' + CONVERT(VARCHAR(40),job_id)+ ''', @delete_history = 1, @delete_unused_schedule=1'
            -- +CHAR(13)+CHAR(10)+'GO'+CHAR(13)+CHAR(10) 
            AS script
        FROM jobs
        ORDER BY job_name, last_start_date""".toString()    



connectionSrv = dbm.getService(ConnectionService.class)

def dbConnections
if (p_servers!=null && p_servers.size()>0) {
    dbConnections = p_servers.collect { serverName -> connectionSrv.findByName(serverName) }
} else {
    dbConnections  = connectionSrv.getConnectionList().findAll { it.driver!="ldap" }
}

def tools = new DbmTools ( dbm, logger, getBinding().out)

if (p_action=="List Jobs") {
    println """<table cellspacing="0" class="simple-table" border="1">
                    <tr style="background-color:#EEE">
                        <th>server_name</th>
                        <th>job_name</th>
                        <th>job_description</th>
                        <th>date_created</th>
                        <th>date_modified</th>
                        <th>job_enabled</th>
                        <th>running</th>
                        <th>category</th>
                        <th>last_start_date</th>
                        <th>next_run_date</th>
                        <th>modified_days_ago</th>
                        <th>run_days_ago</th>
                    </tr> """
}
if (p_action=="Generate Drop Script") {
    println "<pre>"
    println "/* FILTER: \n ${p_filter}\n */"
   
    println "-- SWITCH to SQLCMD mode before running the script"
    println ":on error exit"
}

Closure filter = null
if (p_filter!=null) {
    filter = new GroovyShell().evaluate("{ job -> "+ p_filter +"  }")
}
dbConnections.each { connectionInfo ->
    try {
    
        connection = tools.getConnection ( connectionInfo.name ) 
        connection.setTransactionIsolation(java.sql.Connection.TRANSACTION_READ_UNCOMMITTED)
        
        def sql = new Sql(connection)                
        def rows = sql.rows (QUERY)
        if ( filter!=null ) {
            rows = rows.findAll (filter)
        }
        if (p_action=="List Jobs") {
            rows.each { row ->
                print "<tr>"
                
                print "<td>${ tools.rsToString(row.server_name) }</td>"
                print "<td>${ tools.rsToString(row.job_name) }</td>"
                print "<td>${ tools.rsToString(row.job_description) }</td>"
                print "<td>${ tools.rsToString(row.date_created) }</td>"
                print "<td>${ tools.rsToString(row.date_modified) }</td>"
                print "<td>${ tools.rsToString(row.job_enabled) }</td>"
                print "<td>${ tools.rsToString(row.running) }</td>"
                print "<td>${ tools.rsToString(row.category) }</td>"
                print "<td>${ tools.rsToString(row.last_start_date) }</td>"
                print "<td>${ tools.rsToString(row.next_run_date) }</td>"
                print "<td align=\"right\">${ tools.rsToString(row.modified_days_ago) }</td>"
                print "<td align=\"right\">${ tools.rsToString(row.run_days_ago) }</td>"
                println "</tr>"
            }
        }
        if (p_action=="Generate Drop Script") {
            if (rows.size > 0) {
                println "GO"
                println ":connect ${rows[0].server_name}"
                rows.each { job ->
                    print "-- DELETE JOB \"${job.job_name}\" category \"${tools.rsToString(job.category)}\" "
                    print " modified ${tools.rsToString(job.modified_days_ago)} day(s) ago, "
                    println "last run: ${job.run_days_ago == null ? "never" : job.run_days_ago + " days ago" }"
                    println "exec msdb.dbo.sp_delete_job @job_id ='${job.job_id}', @delete_history = 1, @delete_unused_schedule=1"
                    println ""
                }
                println ""
            } else {
                println "-- No jobs found at server ${connectionInfo.name}"
            }
        }
    } catch (Exception e) {
        def msg = "Cannot retrieve job information"
        logger.error(msg, e)
    }
}
if (p_action=="List Jobs") {
    println "</table>"
}