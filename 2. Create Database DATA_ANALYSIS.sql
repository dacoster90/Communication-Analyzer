--- Creating tables and adding primary keys ---
CREATE TABLE SENT (
	id_sent INT NOT NULL,
	month INT NOT NULL,
	year INT NOT NULL,
	totalsentmessages INT NULL,
	averagesentdelivery VARCHAR(20) NULL,
	percentageSentDelay VARCHAR(10) NULL,
	totalsentdelay INT NULL,
	averagesentdeliverydelay VARCHAR(20) NULL,
	gprs_delay_sent int NULL,
	perc_gprs_delay_sent VARCHAR(10) NULL,
	gprs_non_queue_sent int NULL,
	perc_gprs_non_queue_sent VARCHAR(10) NULL,
	gprs_queue_sent int NULL,
	perc_gprs_queue_sent VARCHAR(10) NULL,
	inmarsat_delay_sent int NULL,
	perc_inmarsat_delay_sent VARCHAR(10) NULL,
	inmarsat_non_queue_sent int NULL,
	perc_inmarsat_non_queue_sent VARCHAR(10) NULL,
	inmarsat_queue_sent int NULL,
	perc_inmarsat_queue_sent VARCHAR(10) NULL,
	no_attempt_sent int NULL,
	perc_no_attempt_sent VARCHAR(10) NULL,
	perc_total_queue_sent VARCHAR(10) NULL,
	perc_total_non_queue_sent VARCHAR(10) NULL,
	regtimestamp DATE NOT NULL
	);

ALTER TABLE SENT ADD PRIMARY KEY (id_sent);


CREATE TABLE RECEIVED (
	id_received INT NOT NULL,
	month INT NOT NULL,
	year INT NOT NULL,
	totalreceivedmessages INT NULL,
	averagereceiveddelivery VARCHAR(20) NULL,
	percentagereceiveddelay VARCHAR(10) NULL,
	totalreceiveddelay INT NULL,
	averagereceiveddeliverydelay VARCHAR(20) NULL,
	gprs_delay_received int NULL,
	perc_gprs_delay_received VARCHAR(10) NULL,
	inmarsat_delay_received int NULL,
	perc_inmarsat_delay_received VARCHAR(10) NULL,
	regtimestamp DATE NOT NULL
	);

ALTER TABLE RECEIVED ADD PRIMARY KEY (id_received);


CREATE TABLE TOTAL (
	id_total INT NOT NULL,
	month INT NOT NULL,
	year INT NOT NULL,
	totalmessages INT NULL,
	averagetotaldelivery VARCHAR(20) NULL,
	percentagetotaldelay VARCHAR(10) NULL,
	totaldelay INT NULL,
	averagedeliverydelay VARCHAR(20) NULL,
	regtimestamp DATE NOT NULL
	);

ALTER TABLE TOTAL ADD PRIMARY KEY (id_total);

CREATE TABLE GRAPHS (
	id_graphs INT NOT NULL,
	month INT NOT NULL,
	year INT NOT NULL,
	density BLOB NULL,
	dispersion_all BLOB NULL,
	dispersion_sent_received BLOB NULL,
	dispersion_total BLOB NULL,
	bar_all BLOB NULL,
	bar_sent BLOB NULL,
	bar_received BLOB NULL,
	bar_total BLOB NULL,
	bar_loco BLOB NULL,
	time_series BLOB NULL,
	time_series_decompose BLOB NULL,
	regtimestamp DATE NOT NULL 
	);

ALTER TABLE GRAPHS ADD PRIMARY KEY (id_graphs);

CREATE TABLE DF_SB (
	id_df_sb INT NOT NULL,
	name varchar(10) NOT NULL,
	month INT NOT NULL,
	year INT NOT NULL,
	counts_sent INT NULL,
	counts_received INT NULL,
	counts_total INT NULL,	
	average_time_sent VARCHAR(20) NULL,	
	average_time_received VARCHAR(20) NULL,
	average_time VARCHAR(20) NULL,
	perc_sent VARCHAR(10) NULL,
	perc_received VARCHAR(10) NULL,
	perc_total VARCHAR(10) NULL,
	regtimestamp DATE NOT NULL
	);

ALTER TABLE DF_SB ADD PRIMARY KEY (id_df_sb);

CREATE TABLE LOCO (
	id_loco INT NOT NULL,
	month INT NOT NULL,
	year INT NOT NULL,
	HM_LOCO_MSG VARCHAR(20) NOT NULL,
	T_SENT INT NULL,
	T_RECEIVED INT NULL,
	TOTAL INT NULL,
	D_SENT INT NULL,
	D_RECEIVED INT NULL,
	D_TOTAL INT NULL,
	PERC_DELAY VARCHAR(10) NULL,
	regtimestamp DATE NOT NULL
	);

ALTER TABLE LOCO ADD PRIMARY KEY (id_loco);

CREATE TABLE TIME_SERIES (
	id_ts INT NOT NULL,
	month INT NOT NULL,
	year INT NOT NULL,
	ts_date VARCHAR(20) NOT NULL,
	sent INT NOT NULL, 
	received INT NOT NULL,
	total INT NOT NULL,
	regtimestamp DATE NOT NULL
	);

ALTER TABLE TIME_SERIES ADD PRIMARY KEY (id_ts);

--- Creating Sequences ---
CREATE SEQUENCE SENT_ID_SEQ START WITH 1 INCREMENT BY 1 NOCACHE;
CREATE SEQUENCE RECEIVED_ID_SEQ START WITH 1 INCREMENT BY 1 NOCACHE;
CREATE SEQUENCE TOTAL_ID_SEQ START WITH 1 INCREMENT BY 1 NOCACHE;
CREATE SEQUENCE GRAPHS_ID_SEQ START WITH 1 INCREMENT BY 1 NOCACHE;
CREATE SEQUENCE DF_SB_ID_SEQ START WITH 1 INCREMENT BY 1 NOCACHE;
CREATE SEQUENCE LOCO_ID_SEQ START WITH 1 INCREMENT BY 1 NOCACHE;
CREATE SEQUENCE TS_ID_SEQ START WITH 1 INCREMENT BY 1 NOCACHE;

--- Uploading Images to Oracle Database ---
create or replace NONEDITIONABLE PROCEDURE graphs_pr (month_param INT, year_param INT, density_param STRING, dispersion_all_param STRING,
dispersion_sent_received_param STRING, dispersion_total_param STRING, bar_all_param STRING, bar_sent_param STRING, bar_received_param STRING,
bar_total_param STRING, bar_loco_param STRING, time_series_param STRING, time_series_decompose_param STRING)
IS

    BEGIN

    UPDATE GRAPHS SET density=BFILENAME('IMAGES',density_param),
    dispersion_all = BFILENAME('IMAGES', dispersion_all_param),
    dispersion_sent_received = BFILENAME('IMAGES', dispersion_sent_received_param),
    dispersion_total = BFILENAME('IMAGES', dispersion_total_param),
    bar_all = BFILENAME('IMAGES', bar_all_param),
    bar_sent = BFILENAME('IMAGES', bar_sent_param),
    bar_received = BFILENAME('IMAGES', bar_received_param),
    bar_total = BFILENAME('IMAGES', bar_total_param),
    bar_loco = BFILENAME('IMAGES', bar_loco_param),
    time_series = BFILENAME('IMAGES', time_series_param),
    time_series_decompose = BFILENAME('IMAGES', time_series_decompose_param)
    WHERE MONTH = month_param and YEAR = year_param;-- CHANGE HERE 'image_1' (name column to insert to)

    COMMIT;
    END;