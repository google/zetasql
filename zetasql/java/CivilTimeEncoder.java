/*
 * Copyright 2019 ZetaSQL Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.google.zetasql;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.zetasql.ZetaSQLValue.ValueProto;
import org.joda.time.LocalDateTime;
import org.joda.time.LocalTime;

/**
 * Encoder for TIME and DATETIME values, according to civil_time encoding.
 *
 * <p>The valid range and number of bits required by each date/time field is as the following:
 *
 * <table>
 *   <tr> <th> Field  </th> <th> Range          </th> <th> #Bits </th> </tr>
 *   <tr> <td> Year   </td> <td> [1, 9999]      </td> <td> 14    </td> </tr>
 *   <tr> <td> Month  </td> <td> [1, 12]        </td> <td> 4     </td> </tr>
 *   <tr> <td> Day    </td> <td> [1, 31]        </td> <td> 5     </td> </tr>
 *   <tr> <td> Hour   </td> <td> [0, 23]        </td> <td> 5     </td> </tr>
 *   <tr> <td> Minute </td> <td> [0, 59]        </td> <td> 6     </td> </tr>
 *   <tr> <td> Second </td> <td> [0, 59]*       </td> <td> 6     </td> </tr>
 *   <tr> <td> Micros </td> <td> [0, 999999]    </td> <td> 20    </td> </tr>
 *   <tr> <td> Nanos  </td> <td> [0, 999999999] </td> <td> 30    </td> </tr>
 * </table>
 *
 * <p>* Leap second is not supported.
 *
 * <p>When encoding the TIME or DATETIME into a bit field, larger date/time field is on the more
 * significant side.
 */
public final class CivilTimeEncoder {
  private static final int NANO_LENGTH = 30;
  private static final int MICRO_LENGTH = 20;

  private static final int NANO_SHIFT = 0;
  private static final int MICRO_SHIFT = 0;
  private static final int SECOND_SHIFT = 0;
  private static final int MINUTE_SHIFT = 6;
  private static final int HOUR_SHIFT = 12;
  private static final int DAY_SHIFT = 17;
  private static final int MONTH_SHIFT = 22;
  private static final int YEAR_SHIFT = 26;

  private static final long NANO_MASK = 0x3FFFFFFFL;
  private static final long MICRO_MASK = 0xFFFFFL;
  private static final long SECOND_MASK = 0x3FL;
  private static final long MINUTE_MASK = 0xFC0L;
  private static final long HOUR_MASK = 0x1F000L;
  private static final long DAY_MASK = 0x3E0000L;
  private static final long MONTH_MASK = 0x3C00000L;
  private static final long YEAR_MASK = 0xFFFC000000L;

  private static final long TIME_SECONDS_MASK = 0x1FFFFL;
  private static final long TIME_MICROS_MASK = 0x1FFFFFFFFFL;
  private static final long TIME_NANOS_MASK = 0x7FFFFFFFFFFFL;
  private static final long DATETIME_SECONDS_MASK = 0xFFFFFFFFFFL;
  private static final long DATETIME_MICROS_MASK = 0xFFFFFFFFFFFFFFFL;

  /**
   * Encodes {@code time} as a 4-byte integer with seconds precision.
   *
   * <p>Encoding is as the following:
   *
   * <pre>
   *      3         2         1
   * MSB 10987654321098765432109876543210 LSB
   *                    | H ||  M ||  S |
   * </pre>
   *
   * @see #decodePacked32TimeSeconds(int)
   */
  public static int encodePacked32TimeSeconds(LocalTime time) {
    checkValidTimeSeconds(time);
    int bitFieldTimeSeconds = 0x0;
    bitFieldTimeSeconds |= time.getHourOfDay() << HOUR_SHIFT;
    bitFieldTimeSeconds |= time.getMinuteOfHour() << MINUTE_SHIFT;
    bitFieldTimeSeconds |= time.getSecondOfMinute() << SECOND_SHIFT;
    return bitFieldTimeSeconds;
  }

  /**
   * Decodes {@code bitFieldTimeSeconds} as a {@link LocalTime} with seconds precision.
   *
   * <p>Encoding is as the following:
   *
   * <pre>
   *      3         2         1
   * MSB 10987654321098765432109876543210 LSB
   *                    | H ||  M ||  S |
   * </pre>
   *
   * @see #encodePacked32TimeSeconds(LocalTime)
   */
  public static LocalTime decodePacked32TimeSeconds(int bitFieldTimeSeconds) {
    checkValidBitField(bitFieldTimeSeconds, TIME_SECONDS_MASK);
    int hourOfDay = getFieldFromBitField(bitFieldTimeSeconds, HOUR_MASK, HOUR_SHIFT);
    int minuteOfHour = getFieldFromBitField(bitFieldTimeSeconds, MINUTE_MASK, MINUTE_SHIFT);
    int secondOfMinute = getFieldFromBitField(bitFieldTimeSeconds, SECOND_MASK, SECOND_SHIFT);
    LocalTime time = new LocalTime(hourOfDay, minuteOfHour, secondOfMinute);
    checkValidTimeSeconds(time);
    return time;
  }

  /**
   * Encodes {@code time} as a 8-byte integer with microseconds precision.
   *
   * <p>Encoding is as the following:
   *
   * <pre>
   *        6         5         4         3         2         1
   * MSB 3210987654321098765432109876543210987654321098765432109876543210 LSB
   *                                | H ||  M ||  S ||-------micros-----|
   * </pre>
   *
   * @see #decodePacked64TimeMicros(long)
   */
  public static long encodePacked64TimeMicros(LocalTime time) {
    checkValidTimeMillis(time);
    return (((long) encodePacked32TimeSeconds(time)) << MICRO_LENGTH)
        | (time.getMillisOfSecond() * 1_000L);
  }

  /**
   * Decodes {@code bitFieldTimeMicros} as a {@link LocalTime} with microseconds precision.
   *
   * <p>Encoding is as the following:
   *
   * <pre>
   *        6         5         4         3         2         1
   * MSB 3210987654321098765432109876543210987654321098765432109876543210 LSB
   *                                | H ||  M ||  S ||-------micros-----|
   * </pre>
   *
   * <p><b>Warning: LocalTime only supports milliseconds precision. Result is truncated.</b>
   *
   * @see #encodePacked64TimeMicros(LocalTime)
   */
  public static LocalTime decodePacked64TimeMicros(long bitFieldTimeMicros) {
    checkValidBitField(bitFieldTimeMicros, TIME_MICROS_MASK);
    int bitFieldTimeSeconds = (int) (bitFieldTimeMicros >> MICRO_LENGTH);
    LocalTime timeSeconds = decodePacked32TimeSeconds(bitFieldTimeSeconds);
    int microOfSecond = getFieldFromBitField(bitFieldTimeMicros, MICRO_MASK, MICRO_SHIFT);
    checkValidMicroOfSecond(microOfSecond);
    LocalTime time = timeSeconds.withMillisOfSecond(microOfSecond / 1_000);
    checkValidTimeMillis(time);
    return time;
  }

  /**
   * Encodes {@code time} as a 8-byte integer with nanoseconds precision.
   *
   * <p>Encoding is as the following:
   *
   * <pre>
   *        6         5         4         3         2         1
   * MSB 3210987654321098765432109876543210987654321098765432109876543210 LSB
   *                      | H ||  M ||  S ||---------- nanos -----------|
   * </pre>
   *
   * @see #decodePacked64TimeNanos(long)
   */
  public static long encodePacked64TimeNanos(LocalTime time) {
    checkValidTimeMillis(time);
    return (((long) encodePacked32TimeSeconds(time)) << NANO_LENGTH)
        | (time.getMillisOfSecond() * 1_000_000L);
  }

  /**
   * Decodes {@code bitFieldTimeNanos} as a {@link LocalTime} with nanoseconds precision.
   *
   * <p>Encoding is as the following:
   *
   * <pre>
   *        6         5         4         3         2         1
   * MSB 3210987654321098765432109876543210987654321098765432109876543210 LSB
   *                      | H ||  M ||  S ||---------- nanos -----------|
   * </pre>
   *
   * <p><b>Warning: LocalTime only supports milliseconds precision. Result is truncated.</b>
   *
   * @see #encodePacked64TimeNanos(LocalTime)
   */
  public static LocalTime decodePacked64TimeNanos(long bitFieldTimeNanos) {
    checkValidBitField(bitFieldTimeNanos, TIME_NANOS_MASK);
    int bitFieldTimeSeconds = (int) (bitFieldTimeNanos >> NANO_LENGTH);
    LocalTime timeSeconds = decodePacked32TimeSeconds(bitFieldTimeSeconds);
    int nanoOfSecond = getFieldFromBitField(bitFieldTimeNanos, NANO_MASK, NANO_SHIFT);
    checkValidNanoOfSecond(nanoOfSecond);
    LocalTime time = timeSeconds.withMillisOfSecond(nanoOfSecond / 1_000_000);
    checkValidTimeMillis(time);
    return time;
  }

  /**
   * Encodes {@code dateTime} as a 8-byte integer with seconds precision.
   *
   * <p>Encoding is as the following:
   *
   * <pre>
   *        6         5         4         3         2         1
   * MSB 3210987654321098765432109876543210987654321098765432109876543210 LSB
   *                             |--- year ---||m || D || H ||  M ||  S |
   * </pre>
   *
   * @see #decodePacked64DatetimeSeconds(long)
   */
  public static long encodePacked64DatetimeSeconds(LocalDateTime dateTime) {
    checkValidDateTimeSeconds(dateTime);
    long bitFieldDatetimeSeconds = 0x0L;
    bitFieldDatetimeSeconds |= (long) dateTime.getYear() << YEAR_SHIFT;
    bitFieldDatetimeSeconds |= (long) dateTime.getMonthOfYear() << MONTH_SHIFT;
    bitFieldDatetimeSeconds |= (long) dateTime.getDayOfMonth() << DAY_SHIFT;
    bitFieldDatetimeSeconds |= (long) encodePacked32TimeSeconds(dateTime.toLocalTime());
    return bitFieldDatetimeSeconds;
  }

  /**
   * Decodes {@code bitFieldDatetimeSeconds} as a {@link LocalDateTime} with seconds precision.
   *
   * <p>Encoding is as the following:
   *
   * <pre>
   *        6         5         4         3         2         1
   * MSB 3210987654321098765432109876543210987654321098765432109876543210 LSB
   *                             |--- year ---||m || D || H ||  M ||  S |
   * </pre>
   *
   * @see #encodePacked64DatetimeSeconds(LocalDateTime)
   */
  public static LocalDateTime decodePacked64DatetimeSeconds(long bitFieldDatetimeSeconds) {
    checkValidBitField(bitFieldDatetimeSeconds, DATETIME_SECONDS_MASK);
    int bitFieldTimeSeconds = (int) (bitFieldDatetimeSeconds & TIME_SECONDS_MASK);
    LocalTime timeSeconds = decodePacked32TimeSeconds(bitFieldTimeSeconds);
    int year = getFieldFromBitField(bitFieldDatetimeSeconds, YEAR_MASK, YEAR_SHIFT);
    int monthOfYear = getFieldFromBitField(bitFieldDatetimeSeconds, MONTH_MASK, MONTH_SHIFT);
    int dayOfMonth = getFieldFromBitField(bitFieldDatetimeSeconds, DAY_MASK, DAY_SHIFT);
    LocalDateTime dateTime =
        new LocalDateTime(
            year,
            monthOfYear,
            dayOfMonth,
            timeSeconds.getHourOfDay(),
            timeSeconds.getMinuteOfHour(),
            timeSeconds.getSecondOfMinute());
    checkValidDateTimeSeconds(dateTime);
    return dateTime;
  }

  /**
   * Encodes {@code dateTime} as a 8-byte integer with microseconds precision.
   *
   * <p>Encoding is as the following:
   *
   * <pre>
   *        6         5         4         3         2         1
   * MSB 3210987654321098765432109876543210987654321098765432109876543210 LSB
   *         |--- year ---||m || D || H ||  M ||  S ||-------micros-----|
   * </pre>
   *
   * @see #decodePacked64DatetimeMicros(long)
   */
  public static long encodePacked64DatetimeMicros(LocalDateTime dateTime) {
    checkValidDateTimeMillis(dateTime);
    return (encodePacked64DatetimeSeconds(dateTime) << MICRO_LENGTH)
        | (dateTime.getMillisOfSecond() * 1_000L);
  }

  /**
   * Decodes {@code bitFieldDatetimeMicros} as a {@link LocalDateTime} with microseconds precision.
   *
   * <p>Encoding is as the following:
   *
   * <pre>
   *        6         5         4         3         2         1
   * MSB 3210987654321098765432109876543210987654321098765432109876543210 LSB
   *         |--- year ---||m || D || H ||  M ||  S ||-------micros-----|
   * </pre>
   *
   * <p><b>Warning: LocalDateTime only supports milliseconds precision. Result is truncated.</b>
   *
   * @see #encodePacked64DatetimeMicros(LocalDateTime)
   */
  public static LocalDateTime decodePacked64DatetimeMicros(long bitFieldDatetimeMicros) {
    checkValidBitField(bitFieldDatetimeMicros, DATETIME_MICROS_MASK);
    long bitFieldDatetimeSeconds = bitFieldDatetimeMicros >> MICRO_LENGTH;
    LocalDateTime dateTimeSeconds = decodePacked64DatetimeSeconds(bitFieldDatetimeSeconds);
    int microOfSecond = getFieldFromBitField(bitFieldDatetimeMicros, MICRO_MASK, MICRO_SHIFT);
    checkValidMicroOfSecond(microOfSecond);
    LocalDateTime dateTime = dateTimeSeconds.withMillisOfSecond(microOfSecond / 1_000);
    checkValidDateTimeMillis(dateTime);
    return dateTime;
  }

  /**
   * Encodes {@code dateTime} as a {@link ValueProto.Datetime}.
   *
   * @see #encodePacked64DatetimeSeconds(LocalDateTime)
   * @see #decodePacked96DatetimeNanos(ValueProto.Datetime)
   */
  public static ValueProto.Datetime encodePacked96DatetimeNanos(LocalDateTime dateTime) {
    checkValidDateTimeMillis(dateTime);
    return ValueProto.Datetime.newBuilder()
        .setBitFieldDatetimeSeconds(encodePacked64DatetimeSeconds(dateTime))
        .setNanos(dateTime.getMillisOfSecond() * 1_000_000)
        .build();
  }

  /**
   * Decodes {@code datetime} as a {@link LocalDateTime}.
   *
   * <p><b>Warning: LocalDateTime only supports milliseconds precision. Result is truncated.</b>
   *
   * @see #decodePacked64DatetimeSeconds(long)
   * @see #encodePacked96DatetimeNanos(LocalDateTime)
   */
  public static LocalDateTime decodePacked96DatetimeNanos(ValueProto.Datetime datetime) {
    checkValidBitField(datetime.getBitFieldDatetimeSeconds(), DATETIME_SECONDS_MASK);
    LocalDateTime dateTimeSeconds =
        decodePacked64DatetimeSeconds(datetime.getBitFieldDatetimeSeconds());
    checkValidNanoOfSecond(datetime.getNanos());
    LocalDateTime dateTime = dateTimeSeconds.withMillisOfSecond(datetime.getNanos() / 1_000_000);
    checkValidDateTimeMillis(dateTime);
    return dateTime;
  }

  private static int getFieldFromBitField(long bitField, long mask, int shift) {
    return (int) ((bitField & mask) >> shift);
  }

  private static void checkValidTimeSeconds(LocalTime time) {
    checkArgument(time.getHourOfDay() >= 0 && time.getHourOfDay() <= 23);
    checkArgument(time.getMinuteOfHour() >= 0 && time.getMinuteOfHour() <= 59);
    checkArgument(time.getSecondOfMinute() >= 0 && time.getSecondOfMinute() <= 59);
  }

  private static void checkValidTimeMillis(LocalTime time) {
    checkValidTimeSeconds(time);
    checkArgument(time.getMillisOfSecond() >= 0 && time.getMillisOfSecond() <= 999);
  }

  private static void checkValidDateTimeSeconds(LocalDateTime dateTime) {
    checkArgument(dateTime.getYear() >= 1 && dateTime.getYear() <= 9999);
    checkArgument(dateTime.getMonthOfYear() >= 1 && dateTime.getMonthOfYear() <= 12);
    checkArgument(dateTime.getDayOfMonth() >= 1 && dateTime.getDayOfMonth() <= 31);
    checkValidTimeSeconds(dateTime.toLocalTime());
  }

  private static void checkValidDateTimeMillis(LocalDateTime dateTime) {
    checkValidDateTimeSeconds(dateTime);
    checkArgument(dateTime.getMillisOfSecond() >= 0 && dateTime.getMillisOfSecond() <= 999);
  }

  private static void checkValidMicroOfSecond(int microOfSecond) {
    checkArgument(microOfSecond >= 0 && microOfSecond <= 999999);
  }

  private static void checkValidNanoOfSecond(int nanoOfSecond) {
    checkArgument(nanoOfSecond >= 0 && nanoOfSecond <= 999999999);
  }

  private static void checkValidBitField(long bitField, long mask) {
    checkArgument((bitField & ~mask) == 0x0L);
  }

  private CivilTimeEncoder() {}
}
