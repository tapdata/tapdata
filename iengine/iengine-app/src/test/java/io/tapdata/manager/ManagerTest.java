package io.tapdata.manager;

import io.tapdata.base.BaseTest;

/**
 * Created by tapdata on 16/01/2018.
 */
public class ManagerTest extends BaseTest {


	private static final String mongoURI = "mongodb://localhost:12345/tapdata";

//    @Test
//    public void pingTimeTest() throws IllegalAccessException, InterruptedException {
//
//        Job nortiwind = getAssignJob("northwind");
//        Calendar calendar = Calendar.getInstance();
//        calendar.add(Calendar.MINUTE, -1);
//        long timeInMillis = calendar.getTimeInMillis();
//        nortiwind.setConnector_ping_time(timeInMillis);
//
//        JobSchedule jobSchedule = new JobSchedule();
//        jobSchedule.setClientMongoOpertor(clientMongoOperator);
//        jobSchedule.setInstanceNo("store-001");
//        jobSchedule.setMongoURI(mongoURI);
//        Job job = jobSchedule.runningJob();
//
//        assert job != null;
//        assert job.getName().equals("northwind");
//
//    }


}
