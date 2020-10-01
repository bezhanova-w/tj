package ru.javawebinar.topjava.util;

import ru.javawebinar.topjava.model.UserMeal;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.stream.Collectors;

public class MealsTable implements Closeable {
    Connection connection;

    MealsTable() throws SQLException, ClassNotFoundException {
        Class.forName("org.hsqldb.jdbcDriver");
        reopenConnection();
    }

    // Закрытие
    public void close() {
        try {
            if (connection != null && !connection.isClosed())
                connection.close();
        } catch (SQLException e) {
            System.out.println("Ошибка закрытия SQL соединения!");
        }
    }

    public void createTable() throws SQLException {
        executeSqlStatement("DROP TABLE meals IF EXISTS; "
                + "CREATE TABLE meals("
                + "datetime TIMESTAMP, "
                + "mdate DATE, "
                + "mtime TIME, "
                + "description VARCHAR, "
                + "calories INT)");
    }

    public void populateTable(List<UserMeal> meals) throws SQLException {
        String values = meals.stream()
                .map(meal -> String.format("%t, %t, %t, '%s', %d",
                        meal.getDateTime(), meal.getDateTime(), meal.getDateTime(), meal.getDescription(), meal.getCalories()))
                .collect(Collectors.joining(","));
        executeSqlStatement("INSERT INTO meals (datetime, mdate, mtime, description, calories) VALUES " + values);
    }

    public void getUserMealsWithExcess() throws SQLException {
        executeSqlStatement("SELECT datetime, calories, description, daily_calories > :limit AS excess " +
                "FROM meals INNER JOIN " +
                "(SELECT mdate, SUM(calories) AS daily_calories FROM meals GROUP BY mdate) AS calories_per_days " +
                "ON meals.mdate = calories_per_days.mdate " +
                "WHERE meals.mtime >= :starttime AND meals.mtime < :endtime");
    }

    void executeSqlStatement(String sql) throws SQLException {
        reopenConnection();
        Statement statement = connection.createStatement();
        statement.execute(sql);
        statement.close();
    };

    // Активизация соединения с СУБД, если оно не активно.
    void reopenConnection() throws SQLException {
        if (connection == null || connection.isClosed()) {
            connection = DriverManager.getConnection("jdbc:hsqldb:mem:connection", "sa", "");;
        }
    }
}
