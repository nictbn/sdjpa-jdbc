package guru.springframework.jdbc.dao;

import guru.springframework.jdbc.domain.Author;
import guru.springframework.jdbc.domain.Book;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.*;

@Component
public class BookDaoImpl implements BookDao {

    private final DataSource source;
    private final AuthorDao authorDao;

    public BookDaoImpl(DataSource source, AuthorDao authorDao) {
        this.source = source;
        this.authorDao = authorDao;
    }

    @Override
    public Book getById(Long id) {
        PreparedStatement preparedStatement = null;
        Connection connection = null;
        ResultSet resultSet = null;
        Book book = null;
        try {
            connection = source.getConnection();
            preparedStatement = connection.prepareStatement("select * from book where id = ?");
            preparedStatement.setLong(1, id);
            resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                book = getBook(resultSet);
            }
        } catch (SQLException exception) {
            exception.printStackTrace();
        } finally {
            closeAll(connection, preparedStatement, resultSet);
        }
        return book;
    }

    @Override
    public Book findByTitle(String title) {
        PreparedStatement preparedStatement = null;
        Connection connection = null;
        ResultSet resultSet = null;
        Book book = null;
        try {
            connection = source.getConnection();
            preparedStatement = connection.prepareStatement("select * from book where title = ?");
            preparedStatement.setString(1, title);
            resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                book = getBook(resultSet);
            }
        } catch (SQLException exception) {
            exception.printStackTrace();
        } finally {
            closeAll(connection, preparedStatement, resultSet);
        }
        return book;
    }

    @Override
    public Book saveNewBook(Book book) {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;

        try {
            connection = source.getConnection();
            preparedStatement = connection.prepareStatement("insert into book (isbn, publisher, title, author_id) values (?, ?, ?, ?)");
            preparedStatement.setString(1, book.getIsbn());
            preparedStatement.setString(2, book.getPublisher());
            preparedStatement.setString(3, book.getTitle());
            if (book.getAuthor() != null) {
                preparedStatement.setLong(4, book.getAuthor().getId());
            } else {
                // -5 means NULL for Bigint numbers
                preparedStatement.setNull(4, -5);
            }
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
            closeAll(connection, preparedStatement, resultSet);
        }
        return null;
    }

    @Override
    public Book updateBook(Book book) {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;

        try {
            connection = source.getConnection();
            preparedStatement = connection.prepareStatement("update book set isbn = ?, publisher = ?, title = ?, author_id = ? where book.id = ?");
            preparedStatement.setString(1, book.getIsbn());
            preparedStatement.setString(2, book.getPublisher());
            preparedStatement.setString(3, book.getTitle());
            if (book.getAuthor() != null) {
                preparedStatement.setLong(4, book.getAuthor().getId());
            }
            preparedStatement.setLong(5, book.getId());
            preparedStatement.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeAll(connection, preparedStatement, resultSet);
        }
        return this.getById(book.getId());
    }

    @Override
    public void deleteBook(Long id) {
        Connection connection = null;
        PreparedStatement preparedStatement = null;

        try {
            connection = source.getConnection();
            preparedStatement = connection.prepareStatement("delete from book where id = ?");
            preparedStatement.setLong(1, id);
            preparedStatement.execute();
        } catch (SQLException exception) {
            exception.printStackTrace();
        } finally {
            closeAll(connection, preparedStatement, null);
        }
    }

    private Book getBook(ResultSet resultSet) {
        Book book = null;
        try {
            Long id = resultSet.getLong(1);
            String title = resultSet.getString("title");
            String isbn = resultSet.getString("isbn");
            String publisher = resultSet.getString("publisher");
            book = new Book();
            book.setId(id);
            book.setTitle(title);
            book.setIsbn(isbn);
            book.setPublisher(publisher);
            book.setAuthor(authorDao.getById(resultSet.getLong("author_id")));
        } catch (SQLException exception) {
            exception.printStackTrace();
        }
        return book;
    }

    private void closeAll(Connection connection, PreparedStatement preparedStatement, ResultSet resultSet) {
        try {
            if (connection != null) {
                connection.close();
            }
            if (preparedStatement != null) {
                preparedStatement.close();
            }
            if (resultSet != null) {
                resultSet.close();
            }
        } catch (SQLException exception) {
            exception.printStackTrace();
        }
    }
}
