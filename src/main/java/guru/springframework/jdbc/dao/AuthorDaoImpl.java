package guru.springframework.jdbc.dao;

import guru.springframework.jdbc.domain.Author;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.*;

@Component
public class AuthorDaoImpl implements AuthorDao {

    private final DataSource source;

    public AuthorDaoImpl(DataSource source) {
        this.source = source;
    }

    @Override
    public Author getById(Long id) {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;

        try {
            connection = source.getConnection();
            preparedStatement = connection.prepareStatement("Select * From author where id = ?");
            preparedStatement.setLong(1, id);
            resultSet = preparedStatement.executeQuery();

            if (resultSet.next()) {
                return getAuthorFromResultSet(resultSet);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeAll(resultSet, preparedStatement, connection);
        }
        return null;
    }

    @Override
    public Author findAuthorByName(String firstName, String lastName) {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;

        try {
            connection = source.getConnection();
            preparedStatement = connection.prepareStatement("Select * From author where first_name = ? and last_name = ?");
            preparedStatement.setString(1, firstName);
            preparedStatement.setString(2, lastName);
            resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                return getAuthorFromResultSet(resultSet);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeAll(resultSet, preparedStatement, connection);
        }
        return null;
    }

    @Override
    public Author saveNewAuthor(Author author) {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;

        try {
            connection = source.getConnection();
            preparedStatement = connection.prepareStatement("insert into author (first_name, last_name) values (?, ?)");
            preparedStatement.setString(1, author.getFirstName());
            preparedStatement.setString(2, author.getLastName());
            preparedStatement.execute();

            Statement statement = connection.createStatement();

            // THIS IS A MYSQL SPECIFIC FUNCTION
            resultSet = statement.executeQuery("SELECT LAST_INSERT_ID()");

            if (resultSet.next()) {
                Long savedId = resultSet.getLong(1);
                return this.getById(savedId);
            }
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeAll(resultSet, preparedStatement, connection);
        }
        return null;
    }

    @Override
    public Author updateAuthor(Author author) {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;

        try {
            connection = source.getConnection();
            preparedStatement = connection.prepareStatement("update author set first_name = ?, last_name = ? where author.id = ?");
            preparedStatement.setString(1, author.getFirstName());
            preparedStatement.setString(2, author.getLastName());
            preparedStatement.setLong(3, author.getId());
            preparedStatement.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeAll(resultSet, preparedStatement, connection);
        }
        return this.getById(author.getId());
    }

    @Override
    public void deleteAuthorById(Long id) {
        Connection connection = null;
        PreparedStatement preparedStatement = null;

        try {
            connection = source.getConnection();
            preparedStatement = connection.prepareStatement("delete from author where id = ?");
            preparedStatement.setLong(1, id);
            preparedStatement.execute();
        } catch (SQLException exception) {
            exception.printStackTrace();
        } finally {
            closeAll(null, preparedStatement, connection);
        }
    }

    private Author getAuthorFromResultSet(ResultSet resultSet) {
        Author author = new Author();
        try {
            author.setId(resultSet.getLong("id"));
            author.setFirstName(resultSet.getString("first_name"));
            author.setLastName(resultSet.getString("last_name"));
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return author;
    }

    private void closeAll(ResultSet resultSet, PreparedStatement preparedStatement, Connection connection) {
        try {
            if (resultSet != null) {
                resultSet.close();
            }
            if (preparedStatement != null) {
                preparedStatement.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
