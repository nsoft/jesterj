-- Assumes existing db called jjtest

DROP TABLE if exists Books;

CREATE TABLE Books (
  id INT NOT NULL PRIMARY KEY ,
  title VARCHAR NOT NULL,
  author_id int NOT NULL,
  summary TEXT NOT NULL,
  publisher VARCHAR ,
  genre VARCHAR
);

DROP TABLE if exists Author;

CREATE TABLE Author (
  id INT NOT NULL PRIMARY KEY ,
  fname VARCHAR NOT NULL,
  lname VARCHAR NOT NULL,
  genres VARCHAR,
  first_pub DATE
);

INSERT INTO Author (id, fname, lname, genres, first_pub) VALUES (1, 'Glen', 'Cook', 'fantasy, sci-fi', null );
INSERT INTO Author (id, fname, lname, genres, first_pub) VALUES (2, 'Issac', 'Asimov', 'sci-fi, non-fiction', null );
INSERT INTO Author (id, fname, lname, genres, first_pub) VALUES (3, 'David', 'Eddings', 'fantasy', null );

INSERT INTO Books (id, title, author_id, summary, publisher, genre ) VALUES  (1, 'The Black Company', 1, 'A gritty band of mercenaries makes their way through politics and intrigue in an empire run by evil wizards and a rebellion led by equally distasteful wizards', 'TOR Fantasy', 'fantasy' );
INSERT INTO Books (id, title, author_id, summary, publisher, genre ) VALUES  (1, 'Shadows Linger', 1, 'Now the elite chosen force of the Empire of the north, the Black Company is sent to the fringes of the empire where a strange black castle built from human remains threatens to unleash an ancient evil. Yet even greater danger presents itself in the form of an old friend who''s betrayal of the empire and their employer endangers them all.', 'TOR Fantasy', 'fantasy' );
INSERT INTO Books (id, title, author_id, summary, publisher, genre ) VALUES  (1, 'The White Rose', 1, 'As the heart and brains of the White Rose''s forces, hiding in the wasteland known as the plain of fear, the Black Company appears to be fading into it''s final chapter. Then strange letters and stranger allies begin arriving. Soon the mystery which might hold the secret the White Rose needs to turn the tide of imperial dominion draws Croaker north into the empire again. Events and forces of nature produce strange bedfellows, a romance and the near destruction of all involved' , 'TOR Fantasy', 'fantasy' );
INSERT INTO Books (id, title, author_id, summary, publisher, genre ) VALUES  (1, 'Pawn of Prophecy', 3, 'Garion is torn from his life as an ordinary farmboy when his Aunt and her friend, the storyteller known as "Old Wolf" take him on a journey in which he discoveres that his friends and his future are far from ordinary.', 'Del Rey', 'fantasy' );
INSERT INTO Books (id, title, author_id, summary, publisher, genre ) VALUES  (1, 'Foundation', 2, 'Hari Seldon, the father of the science of mathematical psychohistory creates a foundation ostensibly dedicated to the collection of all knowledge into an encyclopedia. Yet as the Galactic Empire decays it becomes clear that the Foundation is meant to accomplish much much more than an encyclopedia.', 'Del Rey', 'fantasy' );
