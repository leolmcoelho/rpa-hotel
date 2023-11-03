SELECT DISTINCT r.numreserva,
    TL.DDI,
    TL.DDD,
    TL.NUMERO
FROM RESERVASFRONT r
    LEFT JOIN movimentohospedes MH ON r.idreservasfront = MH.IDRESERVASFRONT
    AND MH.PRINCIPAL = 'S'
    LEFT JOIN TIPOHOSPEDE TH ON TH.IDTIPOHOSPEDE = MH.IDTIPOHOSPEDE
    AND TH.descricao <> 'Sem CashBack'
    LEFT JOIN ENDPESS EP ON MH.IDHOSPEDE = EP.IDPESSOA
    INNER JOIN pessoa PH ON R.IDHOTEL = PH.IDpessoa
    INNER JOIN STATUSRESERVA S ON R.STATUSRESERVA = S.STATUSRESERVA
    INNER JOIN TELENDPESS TL ON EP.idendereco = TL.idendereco
WHERE R.STATUSRESERVA = 3
    AND R.DATAPARTIDAREAL > TO_DATE(TO_CHAR(SYSDATE - 1, 'DD/MM/YYYY'), 'DD/MM/YYYY')
    AND TL.NUMERO IS NOT NULL
ORDER BY r.numreserva