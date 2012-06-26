package org.rhq.server.plugins.metrics.cassandra;

import static org.joda.time.DateTime.now;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

/**
 * @author John Sanda
 */
public class DateTimeServiceTest {

    DateTimeService dateTimeService = new DateTimeService();

    @Test
    public void timestampBefore7DaysShouldBeInRawDataRange() {
        assertTrue(dateTimeService.isInRawDataRange(now().minusHours(1)), "1 hour ago should be in raw data range.");
        assertTrue(dateTimeService.isInRawDataRange(now().minusDays(1)), "1 day ago should be in raw data range.");
        assertTrue(dateTimeService.isInRawDataRange(now().minusDays(5)), "5 days ago should be in raw data range.");
    }

    @Test
    public void timestampAfter7DaysShouldNotBeInRawDataRange() {
        assertFalse(dateTimeService.isInRawDataRange(now().minusDays(7)), "7 days ago should not be in raw data range.");
        assertFalse(dateTimeService.isInRawDataRange(now().minusDays(7).minusSeconds(1)),
            "7 days and 1 second ago should not be in raw data range.");
    }

    @Test
    public void timestampeBefore2WeeksShouldBeIn1HourDataRange() {
        assertTrue(dateTimeService.isIn1HourDataRange(now().minusDays(7)), "7 days ago should be in 1 hour data range");
        assertTrue(dateTimeService.isIn1HourDataRange(now().minusDays(13)),
            "13 days ago should be in 1 hour data range");
    }

    @Test
    public void timestampAfter2WeeksShouldNotBeIn1HourDataRange() {
        assertFalse(dateTimeService.isIn1HourDataRange(now().minusDays(14)),
            "2 weeks ago should not be in 1 hour data range");
        assertFalse(dateTimeService.isIn1HourDataRange(now().minusDays(15)),
            "15 days ago should not be in 1 hour data range");
    }

    @Test
    public void timestampBefore31DaysShouldBeIn6HourDataRange() {
        assertTrue(dateTimeService.isIn6HourDataRnage(now().minusDays(14)),
            "14 days ago should be in 6 hour data range.");
        assertTrue(dateTimeService.isIn6HourDataRnage(now().minusDays(30)),
            "30 days ago should be in 6 hour data range.");
    }

    @Test
    public void timestampAfter31DaysShouldNotBeIn6HourDataRange() {
        assertFalse(dateTimeService.isIn6HourDataRnage(now().minusDays(31)),
            "31 days ago should not be in 6 hour data range.");
        assertFalse(dateTimeService.isIn6HourDataRnage(now().minusDays(32)),
            "32 days ago should not be in 6 hour data range.");
    }

    @Test
    public void timestampBefore365DaysShouldBeIn24HourDataRange() {
        assertTrue(dateTimeService.isIn24HourDataRnage(now().minusDays(31)),
            "31 days ago should be in 24 hour data range.");
        assertTrue(dateTimeService.isIn24HourDataRnage(now().minusDays(364)),
            "364 days ago should be in 24 hour data range.");
    }

    @Test
    public void timestampAfter365DaysShouldNotBeIn24HourDataRange() {
        assertFalse(dateTimeService.isIn24HourDataRnage(now().minusDays(365)),
            "365 days ago should not be in 24 hour data range.");
    }

}
