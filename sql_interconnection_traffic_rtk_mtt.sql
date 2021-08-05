WITH 

RTK_NBK_INIT AS (

                  SELECT 'РТК - Ноябрьск, местное инициирование вызова' AS "Направление", round(sum(N_DURATION_SEC)/60) AS "Min" FROM (
                  select VC_STATION_A, VC_STATION_B,  D_BEGIN, N_DURATION_SEC, n_ab_bytes, n_ba_bytes
                  from AIS_NET.EX_V_CDR EVC
                  where d_begin BETWEEN TO_DATE(:date1, 'DD.MM.YYYY HH24:MI:SS')
                                    AND TO_DATE(:date2, 'DD.MM.YYYY HH24:MI:SS')
                  and not  REGEXP_LIKE (VC_STATION_B,'^553496|^73496|^550|^70|^489|^3|^0') 
                  and N_BA_BYTES in  (41,44,45)  

) a 
--order by  D_BEGIN
),

RTK_NBK_END AS (
		SELECT 'РТК - Ноябрьск, местное завершение вызова' AS "Направление", round(sum(N_DURATION_SEC)/60)  AS "Min" FROM (
		--select VC_STATION_A, VC_STATION_B,  D_BEGIN, N_DURATION_SEC, n_ab_bytes, n_ba_bytes from (
		select VC_STATION_A, VC_STATION_B,  D_BEGIN, N_DURATION_SEC, n_ab_bytes, n_ba_bytes
		from AIS_NET.EX_V_CDR EVC
		where d_begin BETWEEN TO_DATE(:date1, 'DD.MM.YYYY HH24:MI:SS')
                	          AND TO_DATE(:date2, 'DD.MM.YYYY HH24:MI:SS')

		-- Блок для вырезания дублей. Проверить правильность!
		and NOT (REGEXP_LIKE (VC_STATION_B,'^7349637[348]') 
		AND  n_ab_bytes = 12
		and  n_ba_bytes IS NULL)

	 	and      REGEXP_LIKE (VC_STATION_B,'^7349637|^73496397') 
		and      REGEXP_LIKE (VC_STATION_A,'^7') 
		and not  REGEXP_LIKE (VC_STATION_A,'^73496|^734938') 
		and  n_ab_bytes not in (1,7,8) 
		and (n_ab_bytes not in (12) or not REGEXP_LIKE (VC_STATION_A,'^792206')) -- (после корретировок от РТК от 19.12.2019 при исключении всех дублей расхождение 1,52 % - в допуске, но выборку не исправлял) Исключение дублей, но не все. Так совпадает, если все исключить, то будет на много меньше чем у РТК.
		-- and n_duration_sec < 1690  -- кАстыль ёбта!! (применялся в декабре 18го)
   
			 ) b 
		-- where n_ab_bytes = '12'  -- показать скольк бублей лишних в выборке (после корретировок от РТК от 19.12.2019 при исключении всех дублей расхождение 1,52 %)
		-- order by  D_BEGIN
		),

RTK_MUR_INIT AS (
		SELECT 'РТК - Муравленко, местное инициирование вызова' AS "Направление", round(sum(N_DURATION_SEC)/60) AS "Min" FROM (
		-- SELECT VC_STATION_A, VC_STATION_B,  D_BEGIN, N_DURATION_SEC FROM (
		select EVC.VC_STATION_A, EVC.VC_STATION_B,EVC.D_BEGIN, EVC.N_DURATION_SEC, EVC.VC_ROUTE_A, EVC.VC_ROUTE_B, EVC.N_AB_BYTES, EVC.N_BA_BYTES, EVC.VC_REM, SVG1.VC_GOOD_NAME AS CLASS_A, SVG2.VC_GOOD_NAME AS CLASS_B
		from AIS_NET.EX_V_CDR EVC, AIS_NET.SR_V_GOODS SVG1, AIS_NET.SR_V_GOODS SVG2 
		where EVC.N_GOOD_AB_ID=SVG1.N_GOOD_ID
		and EVC.N_GOOD_BA_ID=SVG2.N_GOOD_ID
		and EVC.d_begin BETWEEN TO_DATE(:date1, 'DD.MM.YYYY HH24:MI:SS')
		                    AND TO_DATE(:date2, 'DD.MM.YYYY HH24:MI:SS')
		and REGEXP_LIKE (VC_STATION_A, '^7349386[0-9]{4}|^73493842[6,7][0-9]{2}') 
		and REGEXP_LIKE (VC_STATION_B, '^553493829100|^5587703829100|^553493829000|^553493829029|^553493829102|^553493844500|^553493844555|^553493844550|^55349329111')

	UNION ALL

		select EVC.VC_STATION_A, EVC.VC_STATION_B,EVC.D_BEGIN, EVC.N_DURATION_SEC, EVC.VC_ROUTE_A, EVC.VC_ROUTE_B, EVC.N_AB_BYTES, EVC.N_BA_BYTES, EVC.VC_REM, SVG1.VC_GOOD_NAME AS CLASS_A, SVG2.VC_GOOD_NAME AS CLASS_B
		from AIS_NET.EX_V_CDR EVC, AIS_NET.SR_V_GOODS SVG1, AIS_NET.SR_V_GOODS SVG2 
		where EVC.N_GOOD_AB_ID=SVG1.N_GOOD_ID
		and EVC.N_GOOD_BA_ID=SVG2.N_GOOD_ID
		and EVC.d_begin BETWEEN TO_DATE(:date1, 'DD.MM.YYYY HH24:MI:SS')
                		    AND TO_DATE(:date2, 'DD.MM.YYYY HH24:MI:SS')
		and vc_route_b = 'ROSTELECOM'
		and      REGEXP_LIKE (VC_STATION_A,'^7349386') 
		-- and      REGEXP_LIKE (VC_STATION_B,'^55349386|^559') 
		AND  not REGEXP_LIKE (VC_STATION_B,'^5534938' )  -- непонятно, нахрена ваще исключать внутризону и считать МГ и МН? Этож отдельный договор?
--and SVG1.VC_GOOD_NAME = 'Телефония внутризоновая РТК исх.'
			) c 
		),

RTK_MUR_END AS (
		SELECT 'РТК - Муравленко, местное завершение вызова' AS "Направление", round(sum(N_DURATION_SEC)/60) AS "Min"  FROM (
		--SELECT VC_STATION_A, VC_STATION_B,  D_BEGIN, N_DURATION_SEC, n_ab_bytes, n_ba_bytes FROM (
		select EVC.VC_STATION_A, EVC.VC_STATION_B,EVC.D_BEGIN, EVC.N_DURATION_SEC, EVC.VC_ROUTE_A, EVC.VC_ROUTE_B, EVC.N_AB_BYTES, EVC.N_BA_BYTES, EVC.VC_REM, SVG1.VC_GOOD_NAME AS CLASS_A, SVG2.VC_GOOD_NAME AS CLASS_B
		from AIS_NET.EX_V_CDR EVC, AIS_NET.SR_V_GOODS SVG1, AIS_NET.SR_V_GOODS SVG2 
		where EVC.N_GOOD_AB_ID=SVG1.N_GOOD_ID
		and EVC.N_GOOD_BA_ID=SVG2.N_GOOD_ID
		and EVC.d_begin BETWEEN TO_DATE(:date1, 'DD.MM.YYYY HH24:MI:SS')
                		    AND TO_DATE(:date2, 'DD.MM.YYYY HH24:MI:SS')
		-- and  vc_route_a = 'ROSTELECOM'
		and      REGEXP_LIKE (VC_STATION_B,'^7349386') 
		and      REGEXP_LIKE (VC_STATION_A,'^7|^5534938') 
		and not  REGEXP_LIKE (VC_STATION_A,'^7349637|^7349386') 
			) d 
		),


RTK_VING_INIT AS (
		SELECT 'РТК - Распределение, Выншапуровский, Местное инициированиее' AS "Направление", round(sum(N_DURATION_SEC)/60) AS "Min" FROM (
		-- SELECT * FROM (
		select EVC.VC_STATION_A, EVC.VC_STATION_B,EVC.D_BEGIN, EVC.N_DURATION_SEC, EVC.VC_ROUTE_A, EVC.VC_ROUTE_B, EVC.VC_REM, SVG1.VC_GOOD_NAME AS CLASS_A, SVG2.VC_GOOD_NAME AS CLASS_B
		from AIS_NET.EX_V_CDR EVC, AIS_NET.SR_V_GOODS SVG1, AIS_NET.SR_V_GOODS SVG2 
		where EVC.N_GOOD_AB_ID=SVG1.N_GOOD_ID
		and EVC.N_GOOD_BA_ID=SVG2.N_GOOD_ID
		and EVC.d_begin BETWEEN TO_DATE(:date1, 'DD.MM.YYYY HH24:MI:SS')
                		    AND TO_DATE(:date2, 'DD.MM.YYYY HH24:MI:SS')
		-- and vc_route_b = 'ROSTELECOM'
		-- and  vc_rem = 'NOYABRSK' 
		and      REGEXP_LIKE (VC_STATION_A, '^7349637[0-9]{4}|^73496397[0-9]{3}') 
		and      REGEXP_LIKE (VC_STATION_B, '^553496329100$|^5587706329100$|^5587703829100$|^553496329110$|^553496329000$|^553496329029$|^553496329102$|^553493829000$|^553493829029$|^553493829102$|^553493829100$|^553496365400$|^553496365444$|^553496365455$|^553496365406$|^5534963678[0-9]{2}$|^553496314811$|^553496314877$|^553493844500$|^553493844555$|^553493844550$|^553493844510$|^553496329107$|^553493829111$')
		--ORDER by vc_station_b

	UNION ALL

		select EVC.VC_STATION_A, EVC.VC_STATION_B,EVC.D_BEGIN, EVC.N_DURATION_SEC, EVC.VC_ROUTE_A, EVC.VC_ROUTE_B, EVC.VC_REM, SVG1.VC_GOOD_NAME AS CLASS_A, SVG2.VC_GOOD_NAME AS CLASS_B
		from AIS_NET.EX_V_CDR EVC, AIS_NET.SR_V_GOODS SVG1, AIS_NET.SR_V_GOODS SVG2 
		where EVC.N_GOOD_AB_ID=SVG1.N_GOOD_ID
		and EVC.N_GOOD_BA_ID=SVG2.N_GOOD_ID
		and EVC.d_begin BETWEEN TO_DATE(:date1, 'DD.MM.YYYY HH24:MI:SS')
                		    AND TO_DATE(:date2, 'DD.MM.YYYY HH24:MI:SS')
		-- and vc_route_b = 'ROSTELECOM'
		-- and  vc_rem = 'NOYABRSK' 
		and      REGEXP_LIKE (VC_STATION_A,'^7349637[3,4,8]') 
		and      REGEXP_LIKE (VC_STATION_B,'^55|^7') 
		and not  REGEXP_LIKE (VC_STATION_B,'^553496|^73496|^55349386|^550') 
		and      EVC.VC_ROUTE_B != 'MEGAFON'
		and      SVG1.VC_GOOD_NAME != 'Передача голосовой информации до 100 км исх.'
			) e
		),


RTK_VING_END AS (
		SELECT 'РТК - Распределение, Выншапуровский, Местное Завершение' AS "Направление", round(sum(N_DURATION_SEC)/60) AS "Min" FROM (
		--SELECT * FROM (
		select EVC.VC_STATION_A, EVC.VC_STATION_B,EVC.D_BEGIN, EVC.N_DURATION_SEC, EVC.VC_ROUTE_A, EVC.VC_ROUTE_B, EVC.VC_REM, SVG1.VC_GOOD_NAME AS CLASS_A, SVG2.VC_GOOD_NAME AS CLASS_B
		from AIS_NET.EX_V_CDR EVC, AIS_NET.SR_V_GOODS SVG1, AIS_NET.SR_V_GOODS SVG2 
		where EVC.N_GOOD_AB_ID=SVG1.N_GOOD_ID
		and EVC.N_GOOD_BA_ID=SVG2.N_GOOD_ID
		and EVC.d_begin BETWEEN TO_DATE(:date1, 'DD.MM.YYYY HH24:MI:SS')
                		    AND TO_DATE(:date2, 'DD.MM.YYYY HH24:MI:SS')
		--and vc_route_a = 'ROSTELECOM'
		-- and  vc_rem = 'NOYABRSK' 
		and      REGEXP_LIKE (VC_STATION_B,'^7349637[3,4,8]') 
		and      REGEXP_LIKE (VC_STATION_A,'^7') 
		and not  REGEXP_LIKE (VC_STATION_A,'^73496|^734938') 
		--and SVG1.VC_GOOD_NAME like '%РТК%'
			) f
		),

MTT AS (
		SELECT 'MTT' AS "Направление", sum(N_DURATION_SEC)/60  AS "Min" FROM (
		-- SELECT * FROM (
		select EVC.VC_STATION_A, EVC.VC_STATION_B,EVC.D_BEGIN, EVC.N_DURATION_SEC
		, EVC.VC_ROUTE_A, EVC.VC_ROUTE_B, EVC.N_AB_BYTES, EVC.N_BA_BYTES, EVC.VC_REM, SVG1.VC_GOOD_NAME AS CLASS_A, SVG2.VC_GOOD_NAME AS CLASS_B
		from AIS_NET.EX_V_CDR EVC, AIS_NET.SR_V_GOODS SVG1, AIS_NET.SR_V_GOODS SVG2 
		where EVC.N_GOOD_AB_ID=SVG1.N_GOOD_ID
		and EVC.N_GOOD_BA_ID=SVG2.N_GOOD_ID
		and EVC.d_begin BETWEEN TO_DATE(:date1, 'DD.MM.YYYY HH24:MI:SS')
                		    AND TO_DATE(:date2, 'DD.MM.YYYY HH24:MI:SS')
		-- and vc_route_b = 'NNGS'  
		-- and vc_route_a != 'ROSTELECOM'
		and REGEXP_LIKE (VC_STATION_A,'^7')  
		--  and not REGEXP_LIKE (VC_STATION_A,'^73496|^734938|^70|^7902621|^790262[5-6]|^7902628|^7902693|^7902816|^7902820|^7902824|^7902826|^7902827|^7902829|^7902857|^790445[3-4]|^7904455|^7904457|^7904458|^7904475|^7904485|^7904874|^790688[5-6]|^790849[7-9]|^790885[2-3]|^7908854|^790919[5-9]|^791207[1-3]|^79124[2-3]|^791291|^791955|^79220[5-6]|^792209|^792228|^79224[5-6]|^7929208|^792925|^79519[8-9]|^79615[5-6][0-2]|^796349[6-9]|^796420|^798240|^799240')
		--  AND not REGEXP_LIKE (VC_STATION_A,'^79320[59]')

	-- Отфильтровываем номера сотовых ЯНАО. Разделение условия выборки по регуляркам в зависимости от номера Б, разница в ^79320[59] для НБК. 
	AND (
		REGEXP_LIKE (VC_STATION_B,'^7349386') AND  not REGEXP_LIKE (VC_STATION_A,'^73496|^734938|^70|^7902621|^790262[5-6]|^7902628|^7902693|^7902816|^7902820|^7902824|^7902826|^7902827|^7902829|^7902857|^790445[3-4]|^7904455|^7904457|^7904458|^7904475|^7904485|^7904874|^790688[5-6]|^790849[7-9]|^790885[2-3]|^7908854|^790919[5-9]|^791207[1-3]|^79124[2-3]|^791291|^791955|^79220[5-6]|^792209|^792228|^79224[5-6]|^7929208|^792925|^79519[8-9]|^79615[5-6][0-2]|^796349[6-9]|^796420|^798240|^799240')
		OR  REGEXP_LIKE (VC_STATION_B,'^7349637') AND   not REGEXP_LIKE (VC_STATION_A,'^79320[59]|^73496|^734938|^70|^7902621|^790262[5-6]|^7902628|^7902693|^7902816|^7902820|^7902824|^7902826|^7902827|^7902829|^7902857|^790445[3-4]|^7904455|^7904457|^7904458|^7904475|^7904485|^7904874|^790688[5-6]|^790849[7-9]|^790885[2-3]|^7908854|^790919[5-9]|^791207[1-3]|^79124[2-3]|^791291|^791955|^79220[5-6]|^792209|^792228|^79224[5-6]|^7929208|^792925|^79519[8-9]|^79615[5-6][0-2]|^796349[6-9]|^796420|^798240|^799240')
	    )

		and REGEXP_LIKE (VC_STATION_B,'^7349637|^7349386')
		and EVC.VC_REM = 'NOYABRSK'
		-- and  REGEXP_LIKE (VC_STATION_A,'^73496|^734938') 
		-- and SVG1.VC_GOOD_NAME != 'Передача голосовой информации до 100 км исх.'
		-- and SVG1.VC_GOOD_NAME != 'Телефония корпоративная исх.'
		-- and SVG1.VC_GOOD_NAME = 'Передача голосовой информации на дальние расстояния МГ МТТ. исх.'
		-- and SVG1.VC_GOOD_NAME like '%РТК%'
		and EVC.N_AB_BYTES in (68, 77, 78, 79)
 			) g
 	)


SELECT * FROM MTT

UNION 

SELECT * FROM RTK_NBK_INIT

UNION

SELECT * FROM RTK_NBK_END

UNION

SELECT * FROM RTK_MUR_INIT

UNION

SELECT * FROM RTK_MUR_END

UNION

SELECT * FROM RTK_VING_INIT

UNION

SELECT * FROM RTK_VING_END

UNION

SELECT  'РТК - Распределение, Нобярьск, Местное инициирование', RTK_NBK_INIT."Min" - RTK_VING_INIT."Min" AS min FROM RTK_NBK_INIT, RTK_VING_INIT

UNION 

SELECT  'РТК - Распределение, Нобярьск, Местное Завершение', RTK_NBK_END."Min" - RTK_VING_END."Min" AS min FROM RTK_NBK_END, RTK_VING_END


