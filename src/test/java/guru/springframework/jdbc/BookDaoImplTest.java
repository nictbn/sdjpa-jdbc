package guru.springframework.jdbc;

import guru.springframework.jdbc.dao.BookDao;
import guru.springframework.jdbc.domain.Book;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.ActiveProfiles;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

@ActiveProfiles("local")
@DataJpaTest
@ComponentScan(basePackages = {"guru.springframework.jdbc.dao"})
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
class BookDaoImplTest {

    @Autowired
    BookDao bookDao;

    @Test
    void getById() {
        Book book = bookDao.getById(1L);
        assertThat(book).isNotNull();
    }

    @Test
    void findByTitle() {
        Book book = bookDao.findByTitle("Domain-Driven Design");
        assertThat(book).isNotNull();
    }

    @Test
    void saveBook() {
        Book book = new Book();
        book.setPublisher("O'Reilly");
        book.setIsbn("1234565");
        book.setTitle("Ubung macht den Meister");
        book.setAuthorId(1L);
        Book saved = bookDao.saveNewBook(book);
        assertThat(saved).isNotNull();
    }

    @Test
    void updateBook() {
        Book book = new Book();
        book.setPublisher("O'Reilly");
        book.setIsbn("1234565");
        book.setTitle("Ubung macht den Meister");
        book.setAuthorId(1L);
        Book saved = bookDao.saveNewBook(book);
        assertThat(saved).isNotNull();
        assertThat(saved.getTitle()).isEqualTo("Ubung macht den Meister");

        saved.setTitle("Great Success");
        Book updated = bookDao.updateBook(saved);
        assertThat(updated.getTitle()).isEqualTo("Great Success");
    }

    @Test
    void deleteBook() {
        Book book = new Book();
        book.setPublisher("O'Reilly");
        book.setIsbn("1234565");
        book.setTitle("Ubung macht den Meister");
        book.setAuthorId(1L);
        Book saved = bookDao.saveNewBook(book);

        bookDao.deleteBook(saved.getId());

        Book reloaded = bookDao.getById(saved.getId());
        assertThat(reloaded).isNull();
    }
}