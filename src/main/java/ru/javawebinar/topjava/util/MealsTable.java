package ru.javawebinar.topjava.util;

import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import ru.javawebinar.topjava.model.UserMeal;
import ru.javawebinar.topjava.model.UserMealWithExcess;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

public class MealsTable {
    private static final BeanPropertyRowMapper<UserMealWithExcess> ROW_MAPPER = BeanPropertyRowMapper.newInstance(UserMealWithExcess.class);

    private static final JdbcTemplate jdbcTemplate;

    private static final NamedParameterJdbcTemplate namedParameterJdbcTemplate;

    private static final SimpleJdbcInsert insertUserMeal;

    static {
//        Class.forName("org.hsqldb.jdbcDriver");

        DriverManagerDataSource driverManager = new DriverManagerDataSource("org.hsqldb.jdbcDriver");
        driverManager.setUrl("jdbc:hsqldb:mem:connection");
        driverManager.setUsername("sa");
        driverManager.setPassword("");

        jdbcTemplate = new JdbcTemplate(driverManager);
        namedParameterJdbcTemplate = new NamedParameterJdbcTemplate(jdbcTemplate);
        insertUserMeal = new SimpleJdbcInsert(jdbcTemplate).withTableName("meals");

        jdbcTemplate.execute("DROP TABLE IF EXISTS meals; "
                + "CREATE TABLE meals("
                + "uuid NVARCHAR(255), "
                + "datetime TIMESTAMP, "
                + "mdate DATE, "
                + "mtime TIME, "
                + "description NVARCHAR(255), "
                + "calories INT)");
    }

    public static List<UserMealWithExcess> filterByDatabase(List<UserMeal> meals, LocalTime startTime, LocalTime endTime, int caloriesPerDay) {
        String uuid = UUID.randomUUID().toString();
        populateTable(uuid, meals);
        List<UserMealWithExcess> result = getResultList(uuid, startTime, endTime, caloriesPerDay);
        deleteUsedRows(uuid);
        return result;
    }

    public static void populateTable(String uuid, List<UserMeal> meals) {
        SqlParameterSource[] batch = meals.stream()
                .map(m -> new MapSqlParameterSource()
                        .addValue("uuid", uuid)
                        .addValue("datetime", m.getDateTime())
                        .addValue("mdate", m.getDate())
                        .addValue("mtime", m.getTime())
                        .addValue("description", m.getDescription())
                        .addValue("calories", m.getCalories()))
                .toArray(SqlParameterSource[]::new);
        insertUserMeal.executeBatch(batch);
    }

    private static List<UserMealWithExcess> getResultList(String uuid, LocalTime startTime, LocalTime endTime, int caloriesPerDay) {
        SqlParameterSource parameters = new MapSqlParameterSource()
                .addValue("uuid", uuid)
                .addValue("startTime", startTime)
                .addValue("endTime", endTime)
                .addValue("limit", caloriesPerDay);
        List<UserMealWithExcess> result = namedParameterJdbcTemplate.query(
                "SELECT datetime AS date_time, calories, description, daily_calories > :limit AS excess " +
                "FROM meals INNER JOIN " +
                "(SELECT mdate, SUM(calories) AS daily_calories FROM meals WHERE uuid = :uuid GROUP BY mdate) AS calories_per_days " +
                "ON meals.mdate = calories_per_days.mdate " +
                "WHERE meals.uuid = :uuid AND meals.mtime >= :startTime AND meals.mtime < :endTime", parameters, ROW_MAPPER);
        return result;
    }

    private static void deleteUsedRows(String uuid) {
        jdbcTemplate.update("DELETE FROM meals WHERE uuid = ?", uuid);
    }
}
