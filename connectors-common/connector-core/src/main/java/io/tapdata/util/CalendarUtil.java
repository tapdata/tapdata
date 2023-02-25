package io.tapdata.util;

import java.util.Calendar;

/**
 * Useful Calendar utilities.
 *
 * @author BalusC
 */
public final class CalendarUtil {

  // Init ---------------------------------------------------------------------------------------

  private CalendarUtil() {
    // Utility class, hide the constructor.
  }

  // Validators ---------------------------------------------------------------------------------

  /**
   * Checks whether the given day, month and year combination is a valid date or not.
   *
   * @param year  The year part of the date.
   * @param month The month part of the date.
   * @param day   The day part of the date.
   * @return True if the given day, month and year combination is a valid date.
   */
  public static boolean isValidDate(int year, int month, int day) {
    return isValidDate(year, month, day, 0, 0, 0);
  }

  /**
   * Checks whether the given hour, minute and second combination is a valid time or not.
   *
   * @param hour   The hour part of the time.
   * @param minute The minute part of the time.
   * @param second The second part of the time.
   * @return True if the given hour, minute and second combination is a valid time.
   */
  public static boolean isValidTime(int hour, int minute, int second) {
    return isValidDate(1, 1, 1, hour, minute, second);
  }

  /**
   * Checks whether the given day, month, year, hour, minute and second combination is a valid
   * date or not.
   *
   * @param year   The year part of the date.
   * @param month  The month part of the date.
   * @param day    The day part of the date.
   * @param hour   The hour part of the date.
   * @param minute The minute part of the date.
   * @param second The second part of the date.
   * @return True if the given day, month, year, hour, minute and second combination is a valid
   * date.
   */
  public static boolean isValidDate(
    int year, int month, int day, int hour, int minute, int second) {
    try {
      getValidCalendar(year, month, day, hour, minute, second);
      return true;
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  /**
   * Validate the actual date of the given date elements and returns a calendar instance based on
   * the given date elements. The time is forced to 00:00:00.
   *
   * @param year  The year part of the date.
   * @param month The month part of the date.
   * @param day   The day part of the date.
   * @return A Calendar instance prefilled with the given date elements.
   * @throws IllegalArgumentException If the given date elements does not represent a valid date.
   */
  public static Calendar getValidCalendar(int year, int month, int day) {
    return getValidCalendar(year, month, day, 0, 0, 0);
  }

  /**
   * Validate the actual date of the given date elements and returns a calendar instance based on
   * the given date elements.
   *
   * @param year   The year part of the date.
   * @param month  The month part of the date.
   * @param day    The day part of the date.
   * @param hour   The hour part of the date.
   * @param minute The minute part of the date.
   * @param second The second part of the date.
   * @return A Calendar instance prefilled with the given date elements.
   * @throws IllegalArgumentException If the given date elements does not represent a valid date.
   */
  public static Calendar getValidCalendar(
    int year, int month, int day, int hour, int minute, int second) {
    Calendar calendar = Calendar.getInstance();
    calendar.clear();
    calendar.setLenient(false); // Don't automatically convert invalid date.
    calendar.set(year, month - 1, day, hour, minute, second);
    calendar.getTimeInMillis(); // Lazy update, throws IllegalArgumentException if invalid date.
    return calendar;
  }

  // Changers -----------------------------------------------------------------------------------

  /**
   * Add the given amount of years to the given calendar. The changes are reflected in the given
   * calendar.
   *
   * @param calendar The calendar to add the given amount of years to.
   * @param years    The amount of years to be added to the given calendar. Negative values are also
   *                 allowed, it will just go back in time.
   */
  public static void addYears(Calendar calendar, int years) {
    calendar.add(Calendar.YEAR, years);
  }

  /**
   * Add the given amount of months to the given calendar. The changes are reflected in the given
   * calendar.
   *
   * @param calendar The calendar to add the given amount of months to.
   * @param months   The amount of months to be added to the given calendar. Negative values are
   *                 also allowed, it will just go back in time.
   */
  public static void addMonths(Calendar calendar, int months) {
    calendar.add(Calendar.MONTH, months);
  }

  /**
   * Add the given amount of days to the given calendar. The changes are reflected in the given
   * calendar.
   *
   * @param calendar The calendar to add the given amount of days to.
   * @param days     The amount of days to be added to the given calendar. Negative values are also
   *                 allowed, it will just go back in time.
   */
  public static void addDays(Calendar calendar, int days) {
    calendar.add(Calendar.DATE, days);
  }

  /**
   * Add the given amount of hours to the given calendar. The changes are reflected in the given
   * calendar.
   *
   * @param calendar The calendar to add the given amount of hours to.
   * @param hours    The amount of hours to be added to the given calendar. Negative values are also
   *                 allowed, it will just go back in time.
   */
  public static void addHours(Calendar calendar, int hours) {
    calendar.add(Calendar.HOUR, hours);
  }

  /**
   * Add the given amount of minutes to the given calendar. The changes are reflected in the given
   * calendar.
   *
   * @param calendar The calendar to add the given amount of minutes to.
   * @param minutes  The amount of minutes to be added to the given calendar. Negative values are
   *                 also allowed, it will just go back in time.
   */
  public static void addMinutes(Calendar calendar, int minutes) {
    calendar.add(Calendar.MINUTE, minutes);
  }

  /**
   * Add the given amount of seconds to the given calendar. The changes are reflected in the given
   * calendar.
   *
   * @param calendar The calendar to add the given amount of seconds to.
   * @param seconds  The amount of seconds to be added to the given calendar. Negative values are
   *                 also allowed, it will just go back in time.
   */
  public static void addSeconds(Calendar calendar, int seconds) {
    calendar.add(Calendar.SECOND, seconds);
  }

  /**
   * Add the given amount of millis to the given calendar. The changes are reflected in the given
   * calendar.
   *
   * @param calendar The calendar to add the given amount of millis to.
   * @param millis   The amount of millis to be added to the given calendar. Negative values are
   *                 also allowed, it will just go back in time.
   */
  public static void addMillis(Calendar calendar, int millis) {
    calendar.add(Calendar.MILLISECOND, millis);
  }

  // Comparators --------------------------------------------------------------------------------

  /**
   * Returns <tt>true</tt> if the two given calendars are dated on the same year.
   *
   * @param one The one calendar.
   * @param two The other calendar.
   * @return True if the two given calendars are dated on the same year.
   */
  public static boolean sameYear(Calendar one, Calendar two) {
    return one.get(Calendar.YEAR) == two.get(Calendar.YEAR);
  }

  /**
   * Returns <tt>true</tt> if the two given calendars are dated on the same year and month.
   *
   * @param one The one calendar.
   * @param two The other calendar.
   * @return True if the two given calendars are dated on the same year and month.
   */
  public static boolean sameMonth(Calendar one, Calendar two) {
    return one.get(Calendar.MONTH) == two.get(Calendar.MONTH) && sameYear(one, two);
  }

  /**
   * Returns <tt>true</tt> if the two given calendars are dated on the same year, month and day.
   *
   * @param one The one calendar.
   * @param two The other calendar.
   * @return True if the two given calendars are dated on the same year, month and day.
   */
  public static boolean sameDay(Calendar one, Calendar two) {
    return one.get(Calendar.DATE) == two.get(Calendar.DATE) && sameMonth(one, two);
  }

  /**
   * Returns <tt>true</tt> if the two given calendars are dated on the same year, month, day and
   * hour.
   *
   * @param one The one calendar.
   * @param two The other calendar.
   * @return True if the two given calendars are dated on the same year, month, day and hour.
   */
  public static boolean sameHour(Calendar one, Calendar two) {
    return one.get(Calendar.HOUR_OF_DAY) == two.get(Calendar.HOUR_OF_DAY) && sameDay(one, two);
  }

  /**
   * Returns <tt>true</tt> if the two given calendars are dated on the same year, month, day,
   * hour and minute.
   *
   * @param one The one calendar.
   * @param two The other calendar.
   * @return True if the two given calendars are dated on the same year, month, day, hour and
   * minute.
   */
  public static boolean sameMinute(Calendar one, Calendar two) {
    return one.get(Calendar.MINUTE) == two.get(Calendar.MINUTE) && sameHour(one, two);
  }

  /**
   * Returns <tt>true</tt> if the two given calendars are dated on the same year, month, day,
   * hour, minute and second.
   *
   * @param one The one calendar.
   * @param two The other calendar.
   * @return True if the two given calendars are dated on the same year, month, day, hour, minute
   * and second.
   */
  public static boolean sameSecond(Calendar one, Calendar two) {
    return one.get(Calendar.SECOND) == two.get(Calendar.SECOND) && sameMinute(one, two);
  }

  /**
   * Returns <tt>true</tt> if the two given calendars are dated on the same time. The difference
   * from <tt>one.equals(two)</tt> is that this method does not respect the time zone.
   *
   * @param one The one calendar.
   * @param two The other calendar.
   * @return True if the two given calendars are dated on the same time.
   */
  public static boolean sameTime(Calendar one, Calendar two) {
    return one.getTimeInMillis() == two.getTimeInMillis();
  }

  // Calculators --------------------------------------------------------------------------------

  /**
   * Retrieve the amount of elapsed years between the two given calendars.
   *
   * @param before The first calendar with expected date before the second calendar.
   * @param after  The second calendar with expected date after the first calendar.
   * @return The amount of elapsed years between the two given calendars.
   * @throws IllegalArgumentException If the first calendar is dated after the second calendar.
   */
  public static int elapsedYears(Calendar before, Calendar after) {
    return elapsed(before, after, Calendar.YEAR);
  }

  /**
   * Retrieve the amount of elapsed months between the two given calendars.
   *
   * @param before The first calendar with expected date before the second calendar.
   * @param after  The second calendar with expected date after the first calendar.
   * @return The amount of elapsed months between the two given calendars.
   * @throws IllegalArgumentException If the first calendar is dated after the second calendar.
   */
  public static int elapsedMonths(Calendar before, Calendar after) {
    return elapsed(before, after, Calendar.MONTH);
  }

  /**
   * Retrieve the amount of elapsed days between the two given calendars.
   *
   * @param before The first calendar with expected date before the second calendar.
   * @param after  The second calendar with expected date after the first calendar.
   * @return The amount of elapsed days between the two given calendars.
   * @throws IllegalArgumentException If the first calendar is dated after the second calendar.
   */
  public static int elapsedDays(Calendar before, Calendar after) {
    return elapsed(before, after, Calendar.DATE);
  }

  /**
   * Retrieve the amount of elapsed hours between the two given calendars.
   *
   * @param before The first calendar with expected date before the second calendar.
   * @param after  The second calendar with expected date after the first calendar.
   * @return The amount of elapsed hours between the two given calendars.
   * @throws IllegalArgumentException If the first calendar is dated after the second calendar.
   */
  public static int elapsedHours(Calendar before, Calendar after) {
    return (int) elapsedMillis(before, after, 3600000); // 1h = 60m = 3600s = 3600000ms
  }

  /**
   * Retrieve the amount of elapsed minutes between the two given calendars.
   *
   * @param before The first calendar with expected date before the second calendar.
   * @param after  The second calendar with expected date after the first calendar.
   * @return The amount of elapsed minutes between the two given calendars.
   * @throws IllegalArgumentException If the first calendar is dated after the second calendar.
   */
  public static int elapsedMinutes(Calendar before, Calendar after) {
    return (int) elapsedMillis(before, after, 60000); // 1m = 60s = 60000ms
  }

  /**
   * Retrieve the amount of elapsed seconds between the two given calendars.
   *
   * @param before The first calendar with expected date before the second calendar.
   * @param after  The second calendar with expected date after the first calendar.
   * @return The amount of elapsed seconds between the two given calendars.
   * @throws IllegalArgumentException If the first calendar is dated after the second calendar.
   */
  public static int elapsedSeconds(Calendar before, Calendar after) {
    return (int) elapsedMillis(before, after, 1000); // 1sec = 1000ms.
  }

  /**
   * Retrieve the amount of elapsed milliseconds between the two given calendars.
   *
   * @param before The first calendar with expected date before the second calendar.
   * @param after  The second calendar with expected date after the first calendar.
   * @return The amount of elapsed milliseconds between the two given calendars.
   * @throws IllegalArgumentException If the first calendar is dated after the second calendar.
   */
  public static long elapsedMillis(Calendar before, Calendar after) {
    return elapsedMillis(before, after, 1); // 1ms is apparently 1ms.
  }

  /**
   * Calculate the total of elapsed time from years up to seconds between the two given calendars.
   * It returns an int array with the elapsed years, months, days, hours, minutes and seconds
   * respectively.
   *
   * @param before The first calendar with expected date before the second calendar.
   * @param after  The second calendar with expected date after the first calendar.
   * @return The elapsed time between the two given calendars in years, months, days, hours,
   * minutes and seconds.
   * @throws IllegalArgumentException If the first calendar is dated after the second calendar.
   */
  public static int[] elapsedTime(Calendar before, Calendar after) {
    int[] elapsedTime = new int[6];
    Calendar clone = (Calendar) before.clone(); // Otherwise changes are been reflected.

    elapsedTime[0] = elapsedYears(clone, after);
    addYears(clone, elapsedTime[0]);

    elapsedTime[1] = elapsedMonths(clone, after);
    addMonths(clone, elapsedTime[1]);

    elapsedTime[2] = elapsedDays(clone, after);
    addDays(clone, elapsedTime[2]);

    elapsedTime[3] = elapsedHours(clone, after);
    addHours(clone, elapsedTime[3]);

    elapsedTime[4] = elapsedMinutes(clone, after);
    addMinutes(clone, elapsedTime[4]);

    elapsedTime[5] = elapsedSeconds(clone, after);

    return elapsedTime;
  }

  // Helpers ------------------------------------------------------------------------------------

  /**
   * Retrieve the amount of elapsed time between the two given calendars based on the given
   * calendar field as definied in the Calendar constants, e.g. <tt>Calendar.MONTH</tt>.
   *
   * @param before The first calendar with expected date before the second calendar.
   * @param after  The second calendar with expected date after the first calendar.
   * @param field  The calendar field as definied in the Calendar constants.
   * @return The amount of elapsed time between the two given calendars based on the given
   * calendar field.
   * @throws IllegalArgumentException If the first calendar is dated after the second calendar.
   */
  private static int elapsed(Calendar before, Calendar after, int field) {
    checkBeforeAfter(before, after);
    Calendar clone = (Calendar) before.clone(); // Otherwise changes are been reflected.
    int elapsed = -1;
    while (!clone.after(after)) {
      clone.add(field, 1);
      elapsed++;
    }
    return elapsed;
  }

  /**
   * Retrieve the amount of elapsed milliseconds between the two given calendars and directly
   * divide the outcome by the given factor. E.g.: if the division factor is 1, then you will get
   * the elapsed milliseconds unchanged; if the division factor is 1000, then the elapsed
   * milliseconds will be divided by 1000, resulting in the amount of elapsed seconds.
   *
   * @param before The first calendar with expected date before the second calendar.
   * @param after  The second calendar with expected date after the first calendar.
   * @param factor The division factor which to divide the milliseconds with, expected to be at
   *               least 1.
   * @return The amount of elapsed milliseconds between the two given calendars, divided by the
   * given factor.
   * @throws IllegalArgumentException If the first calendar is dated after the second calendar or
   *                                  if the division factor is less than 1.
   */
  private static long elapsedMillis(Calendar before, Calendar after, int factor) {
    checkBeforeAfter(before, after);
    if (factor < 1) {
      throw new IllegalArgumentException(
        "Division factor '" + factor + "' should not be less than 1.");
    }
    return (after.getTimeInMillis() - before.getTimeInMillis()) / factor;
  }

  /**
   * Check if the first calendar is actually dated before the second calendar.
   *
   * @param before The first calendar with expected date before the second calendar.
   * @param after  The second calendar with expected date after the first calendar.
   * @throws IllegalArgumentException If the first calendar is dated after the second calendar.
   */
  private static void checkBeforeAfter(Calendar before, Calendar after) {
    if (before.after(after)) {
      throw new IllegalArgumentException(
        "The first calendar should be dated before the second calendar.");
    }
  }

}
