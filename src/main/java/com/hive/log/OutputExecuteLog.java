package com.hive.log;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.hive.jdbc.ClosedOrCancelledStatementException;
import org.apache.hive.jdbc.HiveStatement;


/**
 * 输出SQL的Mapreduce的log
 * 
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2017年9月7日 下午3:43:14
 */
public class OutputExecuteLog {

    public static void main(String[] args) {
        // TODO Auto-generated method stub
        String driver = "org.apache.hive.jdbc.HiveDriver";
        
        String parallel = "set hive.exec.parallel = true";
        String sql = "SELECT t0.result_id FROM ( SELECT user_id AS result_id FROM vipjrd.dm_p01_user_flag_jrout AS ta WHERE ( dt = '20170906' ) AND ( ( jr_pcl_user_flag in (5) ) ) GROUP BY user_id ) AS t0 LEFT SEMI JOIN vipjrd.dm_pcl_user_flag_cdi AS t1 ON ( t0.result_id = t1.user_id AND ( t1.dt = '20170906' ) AND ( ( t1.open_mask_flag in ('0100','0101','0102') ) ) )";
        
        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
        }
        
        Connection connnction = null;
        ResultSet rs = null;
        Statement stmt = null;
        try {
            connnction = DriverManager.getConnection("jdbc:hive2://10.199.167.44:10000/hive_vipudp", "hdfs", "");
            stmt = connnction.createStatement();
            //启动日志采集线程
            ExtactLog extactLog = new ExtactLog(stmt);
            Thread t1 = new Thread(extactLog);
            t1.setDaemon(true); //这边可以设置日志采集线程为daemon,这样在执行线程退出以后,可以让日志采集线程退出,避免出现问题
            t1.start();
            
            List<String> result = new ArrayList<>();
            stmt.execute(parallel);
            rs = stmt.executeQuery(sql);
            while(rs.next()) {
                String resultId = rs.getString("result_id");
                result.add(resultId);
            }
            System.out.println("size : " + result.size());
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if(null!=stmt) {
                showRemainIfAny(stmt);
                try {
                    //在关闭之前,再获取一下剩余的日志
                    stmt.close();
                } catch (SQLException e) {
                }
            }
        }
    }
    
    /**
     * 获取剩余的日志信息
     * 运行结束以后,再采集一下剩余的日志
     * @param statement
     */
    public static void showRemainIfAny(Statement statement) {
        if(null!=statement && statement instanceof HiveStatement) {
            HiveStatement hiveStatement = (HiveStatement)statement;
            try {
                List<String> queryLogs = hiveStatement.getQueryLog();
                for (String queryLog : queryLogs) {
                    System.out.println("remain-log : " + queryLog);
                }
            } catch (ClosedOrCancelledStatementException e) {
                e.printStackTrace();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}

/**
 * 日志采集的线程
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2017年9月7日 下午5:12:57
 */
class ExtactLog implements Runnable {
    private Statement statement = null;
    
    public ExtactLog(Statement statement) {
        this.statement = statement;
    }
    
    @Override
    public void run() {
        if(statement instanceof HiveStatement) {
            try {
                HiveStatement hiveStatement = (HiveStatement)statement;
                while(hiveStatement.hasMoreLogs()) {
                    List<String> queryLogs = hiveStatement.getQueryLog();
                    for (String queryLog : queryLogs) {
                        System.out.println("running-log : " + queryLog);
                    }
                }
            } catch (ClosedOrCancelledStatementException e) {
                e.printStackTrace();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        } 
    }
}
