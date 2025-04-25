-- Удаляем все данные из таблиц (сохраняем структуру)
DELETE FROM Messages;
DELETE FROM Matches;
DELETE FROM Interactions;
DELETE FROM Photos;
DELETE FROM Ratings;
DELETE FROM Profiles;
DELETE FROM Users;

-- Сбрасываем последовательност
ALTER SEQUENCE Users_id_seq RESTART WITH 1;
ALTER SEQUENCE Profiles_id_seq RESTART WITH 1;
ALTER SEQUENCE Photos_id_seq RESTART WITH 1;
ALTER SEQUENCE Ratings_id_seq RESTART WITH 1;
ALTER SEQUENCE Interactions_id_seq RESTART WITH 1;
ALTER SEQUENCE Matches_id_seq RESTART WITH 1;
ALTER SEQUENCE Messages_id_seq RESTART WITH 1;