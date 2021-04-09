-- PATIENTS
CREATE TABLE patients (id bigint NOT NULL, PRIMARY KEY (id));

INSERT INTO
    patients(id)
VALUES
    (1),
    (2),
    (3);

-- FORMS
CREATE TABLE forms (
    id bigint NOT NULL,
    patient_id bigint NOT NULL,
    PRIMARY KEY (id),
    FOREIGN KEY (patient_id) REFERENCES patients(id)
);

INSERT INTO
    forms(id, patient_id)
VALUES
    (1, 1),
    (2, 1),
    (3, 2);

-- PATIENTS
CREATE TABLE fields (
    id bigint NOT NULL,
    form_id bigint NOT NULL,
    date date,
    time time,
    PRIMARY KEY (id),
    FOREIGN KEY (form_id) REFERENCES forms(id)
);

INSERT INTO
    fields(id, form_id, date, time)
VALUES
    (1, 1, '01-01-2000', null),
    (2, 1, null, '11:00'),
    (3, 2, '01-02-2000', null),
    (4, 2, null, '12:00'),
    (5, 3, '01-03-2000', null),
    (6, 3, null, '13:00');