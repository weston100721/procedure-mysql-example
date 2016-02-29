/**
  * autor: Wojciech Brüggemann
  * opis:  procedura liczy wartosc umowy i lokalizacji z nia zwiazanych
  */

DELIMITER $$

DROP PROCEDURE IF EXISTS wartosc_umowy_i_lokalizacji_2013_06_12_1564 $$

/**
 * _ID_Umowy VARCHAR255 - id umowy
 * ktore_lokalizacje_sa_liczone ENUM|NULL - ktore lokalizacje sa liczone, jesli NULL wartosc jest odczytywana z pliku "dukat.ini" poprzez tablice "config"
 * ile_miesiecy_naprzod INT|NULL - ile miesiecy naprzod liczyc, jesli NULL wartosc jest odczytywana z pliku "dukat.ini" poprzez tablice "config"
 * wymus_obliczenia_od DATE|NULL - data "od" dla ktorej sa wymuszane obliczenia (dla pelnego miesiaca) niezaleznie czy faktura byla wystawiona czy nie (musi byc tez podany parametr "wymus_obliczenia_do")
 * wymus_obliczenia_do DATE|NULL - data "do" dla ktorej sa wymuszane obliczenia (dla pelnego miesiaca) niezaleznie czy faktura byla wystawiona czy nie (musi byc tez podany parametr "wymus_obliczenia_od")
 * zapisz_obliczenia_w_bazie INT - czy ma zapisac dane w tabelach "umowy_wartosci" i "lokalizacje_wartosci", jesli nie to dane mozna odczytac tylko z tabel w pamieci RAM
 * licz_tylko_od DATE|NULL - czy ma liczyc od danej daty pomijajac obliczenia dla okresow wczesniejszych
 * licz_tylko_do DATE|NULL - czy ma liczyc do danej daty pomijajac obliczenia dla okresow pozniejszych
 */
CREATE PROCEDURE wartosc_umowy_i_lokalizacji_2013_06_12_1564(IN _ID_Umowy VARCHAR(255) CHARACTER SET utf8,
                                                                IN ktore_lokalizacje_sa_liczone
                                                                    ENUM('niezafakturowane',
                                                                            'wszystkie',
                                                                            'wszystkie_i_niezweryfikowane'),
                                                                IN ile_miesiecy_naprzod INT,
                                                                IN wymus_obliczenia_od DATE,
                                                                IN wymus_obliczenia_do DATE,
                                                                IN zapisz_obliczenia_w_bazie TINYINT,
                                                                IN licz_tylko_od DATE,
                                                                IN licz_tylko_do DATE)
    -- READS SQL DATA
BEGIN
    
    DECLARE b TINYINT;
    DECLARE i INT;
    DECLARE is_first TINYINT DEFAULT TRUE;
	
	DECLARE data_od DATE;
	DECLARE data_do DATE;
	DECLARE data_end DATE;
    DECLARE lokalizacja_od DATE;
    DECLARE lokalizacja_do DATE;
    DECLARE umowa_od DATE;
    DECLARE umowa_do DATE;
	DECLARE rok YEAR;
    DECLARE miesiac TINYINT;
    DECLARE dzien_od TINYINT;
    DECLARE dzien_do TINYINT;
    DECLARE ile_dni_w_miesiacu TINYINT;
    DECLARE czy_stawka_eventowa TINYINT DEFAULT FALSE;
--     DECLARE czy_umowa_platna_z_gory TINYINT;
    DECLARE od_kiedy_niezafakturowane DATE;
    DECLARE found_rows INT;
    
    -- umowy
	DECLARE _Umowa_Zawarta_Od DATE;
    DECLARE _Umowa_Zawarta_Do DATE;
    DECLARE _Czy_Odnawialna ENUM('Y','N');
    DECLARE _ID_Typu_Umowy_Platnosc BIGINT;
    DECLARE _platnosci_w_terminie ENUM('Y','N');
    -- lokalizacje
	DECLARE _ID_Lokalizacji BIGINT;
    DECLARE _Data_Zaprzestania_Istnienia DATE;
    DECLARE _czasowa ENUM('Y','N');
    DECLARE _cykliczna ENUM('Y','N');
    DECLARE _start_naliczania DATE;
    DECLARE _koniec_naliczania DATE;
    DECLARE _Stawka_Lok DECIMAL(15,2);
	-- ankieta_lokalizacji
	DECLARE _Dziala_Od DATE;
	
	
    -- glowny kursor
    DECLARE done INT DEFAULT FALSE;
    DECLARE main_cursor CURSOR FOR
        SELECT IF(UNIX_TIMESTAMP(Umowa_Zawarta_Od),Umowa_Zawarta_Od,NULL),
				IF(UNIX_TIMESTAMP(Umowa_Zawarta_Do),Umowa_Zawarta_Do,NULL),
				Czy_Odnawialna,
                ID_Typu_Umowy_Platnosc,
                platnosci_w_terminie,
            l.ID_Lokalizacji,
				IF(UNIX_TIMESTAMP(Data_Zaprzestania_Istnienia),Data_Zaprzestania_Istnienia,NULL),
				czasowa, cykliczna,
				IF(UNIX_TIMESTAMP(start_naliczania),start_naliczania,NULL),
				IF(UNIX_TIMESTAMP(koniec_naliczania),koniec_naliczania,NULL),
                Stawka_Lok,
			IF(UNIX_TIMESTAMP(Dziala_Od),Dziala_Od,NULL)
        FROM umowy u
		JOIN lokalizacje l ON u.ID_Umowy = l.ID_Umowy
		LEFT JOIN ankieta_lokalizacji al ON l.ID_Lokalizacji = al.ID_Lokalizacji
        WHERE u.ID_Umowy = _ID_Umowy
            AND (u.Czy_Zweryfikowany = 'Y' AND l.Czy_Zweryfikowany = 'Y' OR ktore_lokalizacje_sa_liczone = 'wszystkie_i_niezweryfikowane')
        ;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
    
    
    -- ustawienie argumentow procedury
    IF ISNULL(ktore_lokalizacje_sa_liczone) THEN
        SET ktore_lokalizacje_sa_liczone =
            (SELECT `value` FROM `config`
                WHERE `prefix` = 'wartosc_lokalizacji_i_umowy' AND `name` = 'ktore_lokalizacje_sa_liczone');
    END IF;
    IF ISNULL(ile_miesiecy_naprzod) THEN
        SET ile_miesiecy_naprzod =
            (SELECT `value` FROM `config`
                WHERE `prefix` = 'wartosc_lokalizacji_i_umowy' AND `name` = 'ile_miesiecy_naprzod');
    END IF;
    
    
    -- tablica "_config" z ustawieniami globalnymi
    CREATE TEMPORARY TABLE IF NOT EXISTS `_config` (
        `prefix` VARCHAR(64) NOT NULL COMMENT 'prefiks w pliku *.ini',
        `name` VARCHAR(128) NOT NULL COMMENT 'nazwa parametru w pliku *.ini',
        `value` VARCHAR(1024) NOT NULL COMMENT 'wartosc parametru w pliku *.ini',
        PRIMARY KEY (`prefix`,`name`),
        KEY `name` (`name`)
    ) ENGINE=MEMORY DEFAULT CHARSET=utf8;
    IF NOT EXISTS (SELECT NULL FROM _config) THEN
        INSERT INTO _config SELECT * FROM config;
    END IF;
    
    -- tablice "_tmp" do przechowywania dni aktywnych lub nieaktywnych w miesiacu
    CREATE TEMPORARY TABLE IF NOT EXISTS `_tmp` (
        `dzien` TINYINT NOT NULL AUTO_INCREMENT,
        `aktywny` TINYINT NOT NULL DEFAULT 0,
        PRIMARY KEY `dzien` (`dzien`)
    ) ENGINE=MEMORY DEFAULT CHARSET=utf8 AUTO_INCREMENT=1 ;
    CREATE TEMPORARY TABLE IF NOT EXISTS `_tmp2` (
        `dzien` TINYINT NOT NULL AUTO_INCREMENT,
        `aktywny` TINYINT NOT NULL DEFAULT 0,
        PRIMARY KEY `dzien` (`dzien`)
    ) ENGINE=MEMORY DEFAULT CHARSET=utf8 AUTO_INCREMENT=1 ;
    IF NOT EXISTS (SELECT NULL FROM _tmp) THEN
        SET i = 31;
        WHILE i > 0 DO
            INSERT INTO _tmp VALUES ();
            INSERT INTO _tmp2 VALUES ();
            SET i = i - 1;
        END WHILE;
    END IF;

    -- tablice pomocnicze do przechowywania okresow czasowosci i wylaczen dla danej lokalizacji
    CREATE TEMPORARY TABLE IF NOT EXISTS `_czasowosc_umowy` (
        `od` DATE NOT NULL,
        `do` DATE NOT NULL,
        `cykliczna` TINYINT NOT NULL
    ) ENGINE=MEMORY DEFAULT CHARSET=utf8 ;
    CREATE TEMPORARY TABLE IF NOT EXISTS `_czasowosc_lokalizacji` (
        `od` DATE NOT NULL,
        `do` DATE NOT NULL,
        `cykliczna` TINYINT NOT NULL
    ) ENGINE=MEMORY DEFAULT CHARSET=utf8 ;
    CREATE TEMPORARY TABLE IF NOT EXISTS `_wylaczenia_lokalizacji` (
        `od` DATE NOT NULL,
        `do` DATE NOT NULL,
        `cykliczna` TINYINT NOT NULL
    ) ENGINE=MEMORY DEFAULT CHARSET=utf8 ;

    -- tablice pomocnicza do przechowywania stawek dla danej lokalizacji
    CREATE TEMPORARY TABLE IF NOT EXISTS `_stawki` (
        `data_wejscia_w_zycie` DATE NOT NULL,
        `stawka_artysci` DECIMAL(10,2) NOT NULL,
        `stawka_producenci` DECIMAL(10,2) NOT NULL,
        `rabat_sieciowy` DECIMAL(10,2) NOT NULL,
        `rabat_okolicznosciowy` DECIMAL(10,2) NOT NULL,
        PRIMARY KEY (`data_wejscia_w_zycie`)
    ) ENGINE=MEMORY DEFAULT CHARSET=utf8 ;
    CREATE TEMPORARY TABLE IF NOT EXISTS `_stawki2` (
        `data_wejscia_w_zycie` DATE NOT NULL,
        `stawka_artysci` DECIMAL(10,2) NOT NULL,
        `stawka_producenci` DECIMAL(10,2) NOT NULL,
        `rabat_sieciowy` DECIMAL(10,2) NOT NULL,
        `rabat_okolicznosciowy` DECIMAL(10,2) NOT NULL,
        PRIMARY KEY (`data_wejscia_w_zycie`)
    ) ENGINE=MEMORY DEFAULT CHARSET=utf8 ;
    
    -- tablica pomocnicza do przechowywania stawek umowy (suma stawek ze wszystkich lokalizacji)
    DROP TEMPORARY TABLE IF EXISTS `_stawki_umowy`;
    CREATE TEMPORARY TABLE IF NOT EXISTS `_stawki_umowy` (
        `ID_Lokalizacji` INT NOT NULL,
        `data_wejscia_w_zycie` DATE NOT NULL,
        `stawka_artysci` DECIMAL(10,2) NOT NULL,
        `stawka_producenci` DECIMAL(10,2) NOT NULL,
        `rabat_sieciowy` DECIMAL(10,2) NOT NULL,
        `rabat_okolicznosciowy` DECIMAL(10,2) NOT NULL,
        PRIMARY KEY (`ID_Lokalizacji`,`data_wejscia_w_zycie`)
    ) ENGINE=MEMORY DEFAULT CHARSET=utf8 ;
    
    -- tablice pomocnicze to przechowywania wynikow obliczen
    DROP TEMPORARY TABLE IF EXISTS `_umowy_wartosci`;
    CREATE TEMPORARY TABLE `_umowy_wartosci` (
        `ID_Umowy` VARCHAR(50) NOT NULL,
        `rok` SMALLINT(6) NOT NULL,
        `miesiac` TINYINT(4) NOT NULL,
        `wartosc_umowy` DECIMAL(15,2) NOT NULL,
        `wartosc_umowy_artysci` DECIMAL(15,2) NOT NULL,
        `wartosc_umowy_producenci` DECIMAL(15,2) NOT NULL,
        `czy_miesiac_zafakturowany` ENUM('N','Y') NOT NULL,
        `rabat_z_reki` DECIMAL(15,2) NOT NULL,
        `rabat_sieciowy` DECIMAL(10,2) NOT NULL,
        `rabat_okolicznosciowy` DECIMAL(10,2) NOT NULL,
        PRIMARY KEY (`ID_Umowy`,`rok`,`miesiac`)
    ) ENGINE=MEMORY DEFAULT CHARSET=utf8 ;
    DROP TEMPORARY TABLE IF EXISTS `_lokalizacje_wartosci`;
    CREATE TEMPORARY TABLE `_lokalizacje_wartosci` (
        `ID_Lokalizacji` BIGINT(20) NOT NULL,
        `rok` SMALLINT(6) NOT NULL,
        `miesiac` TINYINT(4) NOT NULL,
        `wartosc_lokalizacji` DECIMAL(15,2) NOT NULL,
        `wartosc_lokalizacji_artysci` DECIMAL(15,2) NOT NULL,
        `wartosc_lokalizacji_producenci` DECIMAL(15,2) NOT NULL,
        `rabat_sieciowy` DECIMAL(10,2) NOT NULL,
        `rabat_okolicznosciowy` DECIMAL(10,2) NOT NULL,
        PRIMARY KEY (`ID_Lokalizacji`,`rok`,`miesiac`)
    ) ENGINE=MEMORY DEFAULT CHARSET=utf8 ;
    
    -- tablice pomocnicze to przechowywania poprzednich wynikow obliczen
    DROP TEMPORARY TABLE IF EXISTS `_umowy_wartosci_old`;
    CREATE TEMPORARY TABLE `_umowy_wartosci_old` (
        `ID_Umowy` VARCHAR(50) NOT NULL,
        `rok` SMALLINT(6) NOT NULL,
        `miesiac` TINYINT(4) NOT NULL,
        `wartosc_umowy` DECIMAL(15,2) NOT NULL,
        `wartosc_umowy_artysci` DECIMAL(15,2) NOT NULL,
        `wartosc_umowy_producenci` DECIMAL(15,2) NOT NULL,
        `czy_miesiac_zafakturowany` ENUM('N','Y') NOT NULL,
        `rabat_z_reki` DECIMAL(15,2) NOT NULL,
        `rabat_sieciowy` DECIMAL(10,2) NOT NULL,
        `rabat_okolicznosciowy` DECIMAL(10,2) NOT NULL,
        PRIMARY KEY (`ID_Umowy`,`rok`,`miesiac`)
    ) ENGINE=MEMORY DEFAULT CHARSET=utf8 ;
    
    
    
    OPEN main_cursor;
    main_loop: LOOP
        SET done = FALSE;
        FETCH main_cursor INTO
            _Umowa_Zawarta_Od, _Umowa_Zawarta_Do, _Czy_Odnawialna, _ID_Typu_Umowy_Platnosc, _platnosci_w_terminie,
            _ID_Lokalizacji, _Data_Zaprzestania_Istnienia, _czasowa, _cykliczna, _start_naliczania, _koniec_naliczania, _Stawka_Lok,
			_Dziala_Od
        ;
        IF done THEN
            LEAVE main_loop;
        END IF;
        
        -- przygotowanie tablicy "_czasowosc_umowy"
        -- i inne operacje zwiazane z umowa
        IF is_first THEN
            SET is_first = FALSE;
            
            SET found_rows = (SELECT FOUND_ROWS());
            
            DELETE FROM _czasowosc_umowy WHERE 1;
            IF !ISNULL(_Umowa_Zawarta_Od) && !ISNULL(_Umowa_Zawarta_Do) THEN
                SET b = _Czy_Odnawialna = 'Y';
                IF b && YEAR(_Umowa_Zawarta_Od) != YEAR(_Umowa_Zawarta_Do) THEN
                    INSERT INTO _czasowosc_umowy VALUES (
                        _Umowa_Zawarta_Od,
                        DATE_FORMAT(_Umowa_Zawarta_Od ,'%Y-12-31'),
                        1
                    );
                    INSERT INTO _czasowosc_umowy VALUES (
                        DATE_FORMAT(_Umowa_Zawarta_Do ,'%Y-01-01'),
                        _Umowa_Zawarta_Do,
                        1
                    );
                ELSE
                    INSERT INTO _czasowosc_umowy VALUES (
                        _Umowa_Zawarta_Od,
                        _Umowa_Zawarta_Do,
                        b
                    );
                END IF;
            END IF;
            
            -- ustawienie czy umowa jest platna z gory
--             SET czy_umowa_platna_z_gory = _ID_Typu_Umowy_Platnosc IN (5,6,7,8);
            
            -- ustawienie zmiennych zwiazanych z okresem niezafakturowanym (#2358 notka #6)
            SELECT `Okres_Do`
            INTO od_kiedy_niezafakturowane
            FROM `faktury`
            WHERE `ID_Umowy` = _ID_Umowy
                AND `Czy_Uniewazniona_Korekta` = 'N'
                AND `typ_faktury` = 0
                AND IF(UNIX_TIMESTAMP(`Okres_Do`),1,0)
            ORDER BY `Okres_Do` DESC
            LIMIT 1;
            SET od_kiedy_niezafakturowane = LAST_DAY(od_kiedy_niezafakturowane) + INTERVAL 1 DAY;
            
            -- ustawienia zwiazane z "lokalizacjami niezafakturowanymi"
            IF ktore_lokalizacje_sa_liczone = 'niezafakturowane' THEN
                
                -- pomocnicza tablica ze starymi wartosciami umowy
                INSERT INTO _umowy_wartosci_old
                    SELECT * FROM umowy_wartosci WHERE ID_Umowy = _ID_Umowy;
                
                -- wymuszone obliczanie w zadanym okresie czasu
                IF !ISNULL(wymus_obliczenia_od) && !ISNULL(wymus_obliczenia_do) THEN
                    SET @od_r = YEAR(wymus_obliczenia_od);
                    SET @od_m = MONTH(wymus_obliczenia_od);
                    SET @do_r = YEAR(wymus_obliczenia_do);
                    SET @do_m = MONTH(wymus_obliczenia_do);
                    UPDATE _umowy_wartosci_old o SET czy_miesiac_zafakturowany = 'N'
                    WHERE o.ID_Umowy = _ID_Umowy
                        AND (o.rok = @od_r AND o.miesiac >= @od_m OR o.rok > @od_r)
                        AND (o.rok = @do_r AND o.miesiac <= @do_m OR o.rok < @do_r);
                END IF;
                
            END IF;
            
        END IF;

        -- przygotowanie tablicy "_czasowosc_lokalizacji"
        DELETE FROM _czasowosc_lokalizacji WHERE 1;
        IF _czasowa = 'Y' THEN
            SET b = _cykliczna = 'Y';
            IF b && YEAR(_start_naliczania) != YEAR(_koniec_naliczania) THEN
                INSERT INTO _czasowosc_lokalizacji VALUES (
                    _start_naliczania,
                    DATE_FORMAT(_start_naliczania ,'%Y-12-31'),
                    1
                );
                INSERT INTO _czasowosc_lokalizacji VALUES (
                    DATE_FORMAT(_koniec_naliczania ,'%Y-01-01'),
                    _koniec_naliczania,
                    1
                );
            ELSE
                INSERT INTO _czasowosc_lokalizacji VALUES (
                    _start_naliczania,
                    _koniec_naliczania,
                    b
                );
            END IF;
        END IF;

        -- przygotowanie tablicy "_wylaczenia_lokalizacji"
        DELETE FROM _wylaczenia_lokalizacji WHERE 1;
        INSERT INTO _wylaczenia_lokalizacji
            SELECT
                `poczatek_okresu`,
                IF(cykliczny = 'Y' && YEAR(`poczatek_okresu`) != YEAR(`koniec_okresu`),
                    DATE_FORMAT(`poczatek_okresu` ,'%Y-12-31'), `koniec_okresu`),
                IF(`cykliczny` = 'Y', 1, 0)
            FROM `okresy_wylaczenia`
            WHERE `id_lokalizacji` = _ID_Lokalizacji AND `deleted` = 'N'
            UNION
            SELECT
                IF(cykliczny = 'Y' && YEAR(`poczatek_okresu`) != YEAR(`koniec_okresu`),
                    DATE_FORMAT(`koniec_okresu` ,'%Y-01-01'), `poczatek_okresu`),
                `koniec_okresu`,
                IF(`cykliczny` = 'Y', 1, 0)
            FROM `okresy_wylaczenia`
            WHERE `id_lokalizacji` = _ID_Lokalizacji AND `deleted` = 'N';

        -- przygotowanie tablicy "_stawki"
        DELETE FROM _stawki WHERE 1;
        -- wczytanie stawek dla nowych lokalizacji

        INSERT INTO _stawki
            SELECT DISTINCT
                `data_wejscia_w_zycie`,
                @stawka_artysci:= IF(`stawka_podana_recznie_artysci` > 0, `stawka_podana_recznie_artysci`, `stawka_po_rabatach_artysci`),
                @stawka_producenci:= IF(`stawka_podana_recznie_producenci` > 0, `stawka_podana_recznie_producenci`, `stawka_po_rabatach_producenci`),
                -- rabat_sieciowy
                @rabat_sieciowy:= IF(id_nowej_stawki IS NULL,
                                        ROUND(IF(stawka_po_rabacie_sieciowym = 0,
                                                stawka_za_rozmiar * rabat_sieciowy_procent_z_umowy / 100,
                                                stawka_za_rozmiar - stawka_po_rabacie_sieciowym),
                                        2),
                                        0
                                    ),
                -- rabat_okolicznosciowy
                @rabat_okolicznosciowy:= IF(
                                            id_nowej_stawki IS NULL,
                                                ROUND(stawka_za_rozmiar - @rabat_sieciowy - stawka_po_rabatach, 2),
                                                0
                                            )
            FROM `stawki_lokalizacji`
            WHERE `id_lokalizacji` = _ID_Lokalizacji
            
            -- ten kod powinien byc usuniety
            -- jest to zastepczy sposob na powtarzanie sie stawki dla tej samej daty
            -- ten problem powinien zostac poprawiony w bazie danych
            ON DUPLICATE KEY UPDATE stawka_artysci = @stawka_artysci, stawka_producenci = @stawka_producenci, rabat_sieciowy = @rabat_sieciowy,
                                    rabat_okolicznosciowy = @rabat_okolicznosciowy;
        
        -- wczytanie rownolegle do stawek typu 3 (2) stawek typu 1
        IF EXISTS (SELECT NULL FROM _stawki) THEN
            -- przygotowanie do wgrania starych stawek lokalizacji
            DELETE FROM _stawki2 WHERE 1;
            INSERT INTO _stawki2 SELECT * FROM _stawki;
            DELETE FROM _stawki WHERE 1;
            -- wczytanie stawek dla starych lokalizacji
            CALL _wartosc_umowy_stawki_starych_lokalizacji(_ID_Lokalizacji);
            -- polaczenie stawke typu 1 z typu 3
            REPLACE INTO _stawki SELECT * FROM _stawki2;
        END IF;
        
        IF NOT EXISTS (SELECT NULL FROM _stawki) THEN
            -- wczytanie stawek z tabeli "stawki_eventow"
            STAWKI_EVENTOW: begin
                DECLARE data_wejscia_w_zycie DATE;
                DECLARE okres INT;
                DECLARE stawka_artysci DECIMAL(10,2);
                DECLARE stawka_producenci DECIMAL(10,2);
                DECLARE suma_artysci DECIMAL(10,2);
                DECLARE suma_producenci DECIMAL(10,2);
                DECLARE rabat_sieciowy DECIMAL(10,2);
                DECLARE suma_rabat_sieciowy DECIMAL(10,2);
                DECLARE rabat_okolicznosciowy DECIMAL(10,2);
                DECLARE suma_rabat_okolicznosciowy DECIMAL(10,2);
                
                DECLARE _rok_start SMALLINT;
                DECLARE _miesiac_start TINYINT;
                DECLARE _rok_koniec SMALLINT;
                DECLARE _miesiac_koniec TINYINT;
                DECLARE _stawka_za_event_artysci DECIMAL(10,2);
                DECLARE _stawka_za_event_producenci DECIMAL(10,2);
                DECLARE _rabat_sieciowy DECIMAL(10,2);
                DECLARE _rabat_okolicznosciowy DECIMAL(10,2);
                
				DECLARE done INT DEFAULT FALSE;
				DECLARE inner_cursor CURSOR FOR
                    SELECT `rok_start`,`miesiac_start`,`rok_koniec`,`miesiac_koniec`,
                            IF(`stawka_podana_recznie_artysci` > 0, `stawka_podana_recznie_artysci`, `stawka_po_rabatach_artysci`) * ilosc,
                            (@stawka_producenci:= IF(`stawka_podana_recznie_producenci` > 0, `stawka_podana_recznie_producenci`, `stawka_po_rabatach_producenci`)) * ilosc,
                            -- rabat_sieciowy
                            IF(id_nowej_stawki IS NULL,
                                ROUND((@r:= IF(stawka_po_rabacie_sieciowym = 0,
                                            stawka_za_rozmiar * rabat_sieciowy_procent_z_umowy / 100,
                                            stawka_za_rozmiar - stawka_po_rabacie_sieciowym)) * ilosc, 2),
                                0
                            ),
                            -- rabat_okolicznosciowy
                            IF(id_nowej_stawki IS NULL,
                                ROUND((stawka_za_rozmiar - @r - @stawka_producenci) * ilosc, 2),
                                0
                            )
                    FROM `stawki_eventow`
                    WHERE `id_lokalizacji` = _ID_Lokalizacji AND `stawka_po_rabatach` > 0
                    ORDER BY `rok_start`,`miesiac_start`,`rok_koniec`,`miesiac_koniec`;
					
				DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
                
                OPEN inner_cursor;
                inner_loop: LOOP
                    FETCH inner_cursor
                        INTO _rok_start, _miesiac_start, _rok_koniec, _miesiac_koniec, _stawka_za_event_artysci, _stawka_za_event_producenci,
                                _rabat_sieciowy, _rabat_okolicznosciowy;
                    IF done THEN
                        LEAVE inner_loop;
                    END IF;
                    
                    SET data_wejscia_w_zycie = STR_TO_DATE(CONCAT(_rok_start,_miesiac_start,',1'), '%Y%m,%d');
                    SET suma_artysci = 0;
                    SET suma_producenci = 0;
                    SET suma_rabat_sieciowy = 0;
                    SET suma_rabat_okolicznosciowy = 0;
                    SET okres = PERIOD_DIFF(CONCAT(_rok_koniec,LPAD(_miesiac_koniec,2,0)),
                                            CONCAT(_rok_start,LPAD(_miesiac_start,2,0))) + 1;
                    SET i = okres;
                    WHILE i > 0 DO
                        IF i = 1 THEN
                            /* artysci */
                            SET stawka_artysci = _stawka_za_event_artysci - suma_artysci;
                            /* producenci */
                            SET stawka_producenci = _stawka_za_event_producenci - suma_producenci;
                            SET rabat_sieciowy = _rabat_sieciowy - suma_rabat_sieciowy;
                            SET rabat_okolicznosciowy = _rabat_okolicznosciowy - suma_rabat_okolicznosciowy;
                        ELSE
                            /* artysci */
                            SET stawka_artysci = ROUND(_stawka_za_event_artysci/okres, 2);
                            SET suma_artysci = suma_artysci + stawka_artysci;
                            /* producenci */
                            SET stawka_producenci = ROUND(_stawka_za_event_producenci/okres, 2);
                            SET suma_producenci = suma_producenci + stawka_producenci;
                            
                            SET rabat_sieciowy = ROUND(_rabat_sieciowy/okres, 2);
                            SET suma_rabat_sieciowy = suma_rabat_sieciowy + rabat_sieciowy;
                            
                            SET rabat_okolicznosciowy = ROUND(_rabat_okolicznosciowy/okres, 2);
                            SET suma_rabat_okolicznosciowy = suma_rabat_okolicznosciowy + rabat_okolicznosciowy;
                        END IF;
                        
                        INSERT INTO _stawki VALUES (data_wejscia_w_zycie, stawka_artysci, stawka_producenci, rabat_sieciowy, rabat_okolicznosciowy)
                        ON DUPLICATE KEY UPDATE _stawki.stawka_artysci = _stawki.stawka_artysci + stawka_artysci,
                                                _stawki.stawka_producenci = _stawki.stawka_producenci + stawka_producenci,
                                                _stawki.rabat_sieciowy = _stawki.rabat_sieciowy + rabat_sieciowy,
                                                _stawki.rabat_okolicznosciowy = _stawki.rabat_okolicznosciowy + rabat_okolicznosciowy;
                        
                        SET data_wejscia_w_zycie = DATE_ADD(data_wejscia_w_zycie, INTERVAL 1 MONTH);
                        SET i = i - 1;
                    END WHILE;
                    
                    -- zamyka okres dla stawek
                    IF NOT EXISTS (SELECT NULL FROM _stawki WHERE _stawki.`data_wejscia_w_zycie` = data_wejscia_w_zycie) THEN
                        INSERT INTO _stawki VALUES (data_wejscia_w_zycie, 0, 0, 0, 0);
                    END IF;
                END LOOP;
                CLOSE inner_cursor;
			end STAWKI_EVENTOW;
            
            IF EXISTS (SELECT NULL FROM _stawki) THEN
                SET czy_stawka_eventowa = TRUE;
                -- ustawienie specyficznej daty rozpoczecia i zakonczenia eventu
                SET _Dziala_Od = (SELECT MIN(data_wejscia_w_zycie) FROM _stawki);
                SET _Data_Zaprzestania_Istnienia = (SELECT MAX(data_wejscia_w_zycie) - INTERVAL 1 DAY FROM _stawki);
            ELSE
                
                -- wczytanie stawek dla starych lokalizacji
                CALL _wartosc_umowy_stawki_starych_lokalizacji(_ID_Lokalizacji);
                
                -- wczytanie stawki z tabeli "lokalizacje"
                IF NOT EXISTS (SELECT NULL FROM _stawki) THEN
                    IF !ISNULL(_Stawka_Lok) && _Stawka_Lok > 0 THEN
                        IF ISNULL(_Dziala_Od) THEN
                            SET _Dziala_Od = (SELECT MIN(`Data_Wejscia_W_Zycie`) FROM `zmiany_umow`
                                                WHERE `ID_Umowy` = _ID_Umowy AND IF(UNIX_TIMESTAMP(`Data_Wejscia_W_Zycie`),1,0));
                        END IF;
                        IF _Dziala_Od THEN
                            INSERT INTO _stawki VALUES (
                                _Dziala_Od,
                                0,
                                _Stawka_Lok,
                                0, 0
                            );
                        END IF;
                    END IF;
                    
                    -- wczytanie stawki z tabeli "zmiany_umow"
                    -- ale tylko jesli umowa ma tylko jedna lokalizacje
                    IF NOT EXISTS (SELECT NULL FROM _stawki) && found_rows = 1 THEN
                        INSERT INTO _stawki
                            SELECT `Data_Wejscia_W_Zycie`, 0,
                                    IF(ISNULL(`Stawka_Event`) || !ISNULL(`Stawka_Ryczaltowa`) && `Stawka_Ryczaltowa` > `Stawka_Event`, `Stawka_Ryczaltowa`, `Stawka_Event`),
                                    0, 0
                            FROM `zmiany_umow`
                            WHERE `ID_Umowy` = _ID_Umowy AND (!ISNULL(`Stawka_Ryczaltowa`) OR !ISNULL(`Stawka_Event`))
                            UNION
                            SELECT `Obowiazuje_Do` + INTERVAL 1 DAY, 0,
                                    0,
                                    0, 0
                            FROM `zmiany_umow`
                            WHERE `ID_Umowy` = _ID_Umowy
                            AND IF(UNIX_TIMESTAMP(`Obowiazuje_Do`),1,0)
                            AND `Obowiazuje_Do` + INTERVAL 1 DAY
                                NOT IN (SELECT `Data_Wejscia_W_Zycie` FROM `zmiany_umow`
                                        WHERE `ID_Umowy` = _ID_Umowy);
                    END IF;
                    
                    -- jesli nie zostaly znalezione zadne stawki
                    IF NOT EXISTS (SELECT NULL FROM _stawki) THEN
                        -- zachowuje sie jak "continue"
                        ITERATE main_loop;
                    END IF;
                END IF;
            END IF;
        END IF;
        DELETE FROM _stawki2 WHERE 1;
        INSERT INTO _stawki2
            SELECT * FROM _stawki;
        INSERT INTO _stawki_umowy
            SELECT _ID_Lokalizacji, s.* FROM _stawki s;
        
        
        
        -- ustawienie "lokalizacja_od"
        IF ISNULL(_Dziala_Od) THEN
            SET lokalizacja_od = (SELECT MIN(`data_wejscia_w_zycie`) FROM _stawki);
        ELSE
            SET lokalizacja_od = _Dziala_Od;
        END IF;
        
        -- ustawienie "lokalizacja_do"
        SET lokalizacja_do = IFNULL(_Data_Zaprzestania_Istnienia, LAST_DAY(CURDATE() + INTERVAL ile_miesiecy_naprzod MONTH));

        -- ustawienie dat "od" i "do" dla umowy
        IF ISNULL(umowa_od) || umowa_od > lokalizacja_od THEN
            SET umowa_od = lokalizacja_od;
        END IF;
        IF ISNULL(umowa_do) || umowa_do < lokalizacja_do THEN
            SET umowa_do = lokalizacja_do;
        END IF;
        
        
        IF zapisz_obliczenia_w_bazie THEN
            -- usuniecie ewentualnych poprzednich obliczen dla lokalizacji
            SET @r_od = YEAR(lokalizacja_od);
            SET @r_do = YEAR(lokalizacja_do);
            DELETE FROM lokalizacje_wartosci
            USING lokalizacje_wartosci
                LEFT JOIN _umowy_wartosci_old o ON o.rok = lokalizacje_wartosci.rok AND o.miesiac = lokalizacje_wartosci.miesiac
            WHERE lokalizacje_wartosci.ID_Lokalizacji = _ID_Lokalizacji
                AND
                (
                    (lokalizacje_wartosci.rok = @r_od AND lokalizacje_wartosci.miesiac < MONTH(lokalizacja_od)
                        OR lokalizacje_wartosci.rok < @r_od)
                    OR
                    (lokalizacje_wartosci.rok = @r_do AND lokalizacje_wartosci.miesiac > MONTH(lokalizacja_do)
                        OR lokalizacje_wartosci.rok > @r_do)
                )
                AND (ktore_lokalizacje_sa_liczone != 'niezafakturowane'
                        OR o.czy_miesiac_zafakturowany = 'N'
                        OR CONCAT(lokalizacje_wartosci.rok, LPAD(lokalizacje_wartosci.miesiac,2,'0'))
                            BETWEEN EXTRACT(YEAR_MONTH FROM wymus_obliczenia_od) AND EXTRACT(YEAR_MONTH FROM wymus_obliczenia_do)
                );
        END IF;
        
        -- ustawienie "data_od"
        SET data_od = IF(licz_tylko_od > lokalizacja_od, licz_tylko_od, lokalizacja_od);
        
        -- ustawienie "data_end"
        SET data_end = IF(licz_tylko_do < lokalizacja_do, licz_tylko_do, lokalizacja_do);
        
        
        

        
        /* tu nastepuje obrot o jeden miesiac */
		WHILE data_od <= data_end DO
			
			SET data_do = LAST_DAY(data_od);
            SET ile_dni_w_miesiacu = DAY(data_do);
            IF data_do > data_end THEN
                SET data_do = data_end;
            END IF;
            SET rok = YEAR(data_od);
            SET miesiac = MONTH(data_od);
            SET dzien_od = DAY(data_od);
            SET dzien_do = DAY(data_do);
            
            IF NOT EXISTS (SELECT NULL FROM _umowy_wartosci_old o
                            WHERE o.ID_Umowy = _ID_Umowy AND o.rok = rok AND o.miesiac = miesiac AND o.czy_miesiac_zafakturowany = 'Y')
            THEN
                

                -- wykorzystuje tablice "_tmp"
                OKRES_DZIALANIA_LOKALIZACJI: begin
                    UPDATE _tmp SET aktywny = IF(dzien BETWEEN dzien_od AND dzien_do, 1, 0);
                end OKRES_DZIALANIA_LOKALIZACJI;
                
                -- wykorzystuje tablice "_tmp2"
                CZASOWOSC_LOKALIZACJI: begin
                    DECLARE _od DATE;
                    DECLARE _do DATE;

                    DECLARE done INT DEFAULT FALSE;
                    DECLARE inner_cursor CURSOR FOR
                        SELECT IF(`cykliczna`, `od`+INTERVAL(rok-YEAR(`od`))YEAR, `od`),
                                IF(`cykliczna`, `do`+INTERVAL(rok-YEAR(`do`))YEAR, `do`)
                        FROM _czasowosc_lokalizacji
                        -- zwraca tylko okres ktory ma zwiazek z rozpatrywanym miesiacem
                        WHERE data_od <= IF(`cykliczna`, `do`+INTERVAL(rok-YEAR(`do`))YEAR, `do`)
                                AND data_do >= IF(`cykliczna`, `od`+INTERVAL(rok-YEAR(`od`))YEAR, `od`);

                    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

                    IF !czy_stawka_eventowa && EXISTS (SELECT NULL FROM _czasowosc_lokalizacji) THEN
                        -- czyszczenie tablicy "_tmp2"
                        UPDATE _tmp2 SET aktywny = 0;
                        
                        OPEN inner_cursor;
                        inner_loop: LOOP
                            FETCH inner_cursor INTO _od, _do;
                            IF done THEN
                                LEAVE inner_loop;
                            END IF;

                            UPDATE _tmp2 SET aktywny=1
                            WHERE dzien BETWEEN IF(_od < data_od, dzien_od, DAY(_od))
                                                AND IF(_do > data_do, dzien_do, DAY(_do));
                        END LOOP;
                        CLOSE inner_cursor;
                    ELSE
                        -- jesli nie ma czasowosci lokalizacji to wszystkie dni sa aktywne
                        UPDATE _tmp2 SET aktywny = 1;
                    END IF;
                end CZASOWOSC_LOKALIZACJI;
                

                
                -- polaczenie OKRES_DZIALANIA_LOKALIZACJI z CZASOWOSC_LOKALIZACJI (wynikiem jest czesc wspolna)
                -- wykorzystuje tablice "_tmp2"
                UPDATE _tmp2 SET aktywny = (SELECT aktywny FROM _tmp WHERE dzien = _tmp2.dzien)
                WHERE aktywny = 1;
                
                -- wykorzystuje tablice "_tmp"
                CZASOWOSC_UMOWY: begin
                    DECLARE _od DATE;
                    DECLARE _do DATE;

                    DECLARE done INT DEFAULT FALSE;
                    DECLARE inner_cursor CURSOR FOR
                        SELECT IF(`cykliczna`, `od`+INTERVAL(rok-YEAR(`od`))YEAR, `od`),
                                IF(`cykliczna`, `do`+INTERVAL(rok-YEAR(`do`))YEAR, `do`)
                        FROM _czasowosc_umowy
                        -- zwraca tylko okres ktory ma zwiazek z rozpatrywanym miesiacem
                        WHERE data_od <= IF(`cykliczna`, `do`+INTERVAL(rok-YEAR(`do`))YEAR, `do`)
                                AND data_do >= IF(`cykliczna`, `od`+INTERVAL(rok-YEAR(`od`))YEAR, `od`);

                    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

                    IF !czy_stawka_eventowa && EXISTS (SELECT NULL FROM _czasowosc_umowy) THEN
                        -- czyszczenie tablicy "_tmp"
                        UPDATE _tmp SET aktywny = 0;

                        OPEN inner_cursor;
                        inner_loop: LOOP
                            FETCH inner_cursor INTO _od, _do;
                            IF done THEN
                                LEAVE inner_loop;
                            END IF;

                            UPDATE _tmp SET aktywny=1
                            WHERE dzien BETWEEN IF(_od < data_od, dzien_od, DAY(_od)) AND IF(_do > data_do, dzien_do, DAY(_do));
                        END LOOP;
                        CLOSE inner_cursor;
                    ELSE
                        -- jesli nie ma czasowosci umowy to wszystkie dni sa aktywne
                        UPDATE _tmp SET aktywny = 1;
                    END IF;
                end CZASOWOSC_UMOWY;
                

    
                -- polaczenie CZASOWOSC_UMOWY z wczesniej polaczonym OKRES_DZIALANIA_LOKALIZACJI i CZASOWOSC_LOKALIZACJI
                -- (wynikiem jest czesc wspolna)
                -- wykorzystuje tablice "_tmp"
                UPDATE _tmp SET aktywny = (SELECT aktywny FROM _tmp2 WHERE dzien = _tmp.dzien)
                WHERE aktywny = 1;
                
                -- wykorzystuje tablice "_tmp"
                WYLACZENIA_LOKALIZACJI: begin
                    DECLARE _od DATE;
                    DECLARE _do DATE;

                    DECLARE done INT DEFAULT FALSE;
                    DECLARE inner_cursor CURSOR FOR
                        SELECT IF(`cykliczna`, `od`+INTERVAL(rok-YEAR(`od`))YEAR, `od`),
                                IF(`cykliczna`, `do`+INTERVAL(rok-YEAR(`do`))YEAR, `do`)
                        FROM _wylaczenia_lokalizacji
                        -- zwraca tylko okres ktory ma zwiazek z rozpatrywanym miesiacem
                        WHERE data_od <= IF(`cykliczna`, `do`+INTERVAL(rok-YEAR(`do`))YEAR, `do`)
                                AND data_do >= IF(`cykliczna`, `od`+INTERVAL(rok-YEAR(`od`))YEAR, `od`);

                    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
                    OPEN inner_cursor;
                    inner_loop: LOOP
                        FETCH inner_cursor INTO _od, _do;
                        IF done THEN
                            LEAVE inner_loop;
                        END IF;

                        UPDATE _tmp SET aktywny=0
                        WHERE dzien BETWEEN IF(_od < data_od, dzien_od, DAY(_od)) AND IF(_do > data_do, dzien_do, DAY(_do));

                    END LOOP;
                    CLOSE inner_cursor;
                end WYLACZENIA_LOKALIZACJI;

                STAWKA_LOKALIZACJI: begin
                    DECLARE ilosc_dni_aktywnych TINYINT;
                    DECLARE wartosc_miesieczna_artysci DOUBLE(17,4) DEFAULT 0;
                    DECLARE wartosc_miesieczna_producenci DOUBLE(17,4) DEFAULT 0;
                    DECLARE rabat_sieciowy_miesieczny DOUBLE(17,4) DEFAULT 0;
                    DECLARE rabat_okolicznosciowy_miesieczny DOUBLE(17,4) DEFAULT 0;

                    DECLARE _data_wejscia_w_zycie DATE;
                    DECLARE _stawka_artysci DECIMAL(10,2);
                    DECLARE _stawka_producenci DECIMAL(10,2);
                    DECLARE _start TINYINT;
                    DECLARE _rabat_sieciowy DECIMAL(10,2);
                    DECLARE _rabat_okolicznosciowy DECIMAL(10,2);

                    DECLARE done INT DEFAULT FALSE;
                    DECLARE inner_cursor CURSOR FOR
                        SELECT `data_wejscia_w_zycie`,
                                `stawka_artysci`,
                                `stawka_producenci`,
                                IF(DATE_FORMAT(`data_wejscia_w_zycie`,'%Y%m') = DATE_FORMAT(data_od,'%Y%m'),
                                    DAY(`data_wejscia_w_zycie`), 1) AS `start`,
                                `rabat_sieciowy`,
                                `rabat_okolicznosciowy`
                        FROM `_stawki`
                        WHERE
                        (
                            DATE_FORMAT(`data_wejscia_w_zycie`,'%Y%m') = DATE_FORMAT(data_od,'%Y%m')
                            OR
                            `data_wejscia_w_zycie` = (SELECT `data_wejscia_w_zycie` FROM `_stawki2`
                                                        WHERE `data_wejscia_w_zycie` <= DATE_FORMAT(data_od,'%Y-%m-01')
                                                        ORDER BY `data_wejscia_w_zycie` DESC LIMIT 1)
                        )
                        ORDER BY `data_wejscia_w_zycie` DESC;

                    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
                    SET i = ile_dni_w_miesiacu;
                    OPEN inner_cursor;
                    inner_loop: LOOP
                        FETCH inner_cursor INTO _data_wejscia_w_zycie, _stawka_artysci, _stawka_producenci, _start, _rabat_sieciowy, _rabat_okolicznosciowy;
                        IF done THEN
                            LEAVE inner_loop;
                        END IF;

                        SET ilosc_dni_aktywnych = (SELECT COUNT(*) FROM _tmp WHERE dzien BETWEEN _start AND i AND aktywny = 1);
                        SET wartosc_miesieczna_artysci =
                                wartosc_miesieczna_artysci +
                                (_stawka_artysci / ile_dni_w_miesiacu) -- stawka dzienna
                                *
                                ilosc_dni_aktywnych -- ilosc dni aktywnych w czasie obowiazywania stawki
                                ;
                        SET wartosc_miesieczna_producenci =
                                wartosc_miesieczna_producenci +
                                (_stawka_producenci / ile_dni_w_miesiacu) -- stawka dzienna
                                *
                                ilosc_dni_aktywnych -- ilosc dni aktywnych w czasie obowiazywania stawki
                                ;
                        SET rabat_sieciowy_miesieczny =
                                rabat_sieciowy_miesieczny + (_rabat_sieciowy / ile_dni_w_miesiacu) * ilosc_dni_aktywnych;
                        SET rabat_okolicznosciowy_miesieczny =
                                rabat_okolicznosciowy_miesieczny + (_rabat_okolicznosciowy / ile_dni_w_miesiacu) * ilosc_dni_aktywnych;    
                        
                        SET i = _start - 1;
                    END LOOP;
                    CLOSE inner_cursor;

                    -- zapisanie wartosci lokalizacji w pamieci RAM
                    SET wartosc_miesieczna_artysci = ROUND(wartosc_miesieczna_artysci,2);
                    SET wartosc_miesieczna_producenci = ROUND(wartosc_miesieczna_producenci,2);
                    INSERT INTO _lokalizacje_wartosci
                    VALUES (_ID_Lokalizacji, rok, miesiac, wartosc_miesieczna_artysci+wartosc_miesieczna_producenci,
                                wartosc_miesieczna_artysci, wartosc_miesieczna_producenci,
                                ROUND(rabat_sieciowy_miesieczny,2),
                                ROUND(rabat_okolicznosciowy_miesieczny,2)
                        );

                end STAWKA_LOKALIZACJI;
            
            END IF;
			
			
			SET data_od = DATE_ADD(data_do, INTERVAL 1 DAY);
			
		END WHILE;
        
    END LOOP;
    CLOSE main_cursor;
    
    
    
    -- koniec fragmentu procedury

END $$

DELIMITER ;

-- CALL wartosc_umowy_i_lokalizacji_2013_06_12_1564('2014/1012/00001','wszystkie_i_niezweryfikowane',12,NULL,NULL,1,NULL,NULL);
-- CALL wszystkie_wartosci_umow_i_lokalizacji_2013_07_28_1564();
