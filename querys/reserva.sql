SELECT distinct R.NUMRESERVA,
    R.CODREFERENCIA,
    r.DATARESERVA,
    r.datachegadareal,
    r.datapartidareal,
    SG.DESCRICAO SEGMENTO,
    m.descricao MEIO,
    v.descricao veiculo,
    O.DESCRICAO ORIGEM,
    R.CODUH,
    TO_NUMBER(
        SUM(
            DECODE(
                T.IDGRUPODC,
                PH.IDGRUPODCCREDITO,
                0,
                DECODE(T.GRUPOFIXO, 'DI', L.VLRLANCAMENTO, 0)
            )
        )
    ) AS DIARIA,
    TO_NUMBER(
        SUM(
            DECODE(
                T.IDGRUPODC,
                PH.IDGRUPODCCREDITO,
                0,
                DECODE(
                    T.GRUPOFIXO,
                    'BU',
                    L.VLRLANCAMENTO,
                    'BA',
                    L.VLRLANCAMENTO,
                    'BQ',
                    L.VLRLANCAMENTO,
                    'RS',
                    L.VLRLANCAMENTO,
                    'RE',
                    L.VLRLANCAMENTO,
                    0
                )
            )
        )
    ) AS CONSUMOS,
    T1.DESCRICAO NM_TARIFA,
    e.nomeempresa,
    P.nome,
    p.email,
    p.numdocumento,
    TP.NOMEDOCUMENTO
FROM NOTAFRONT N,
    LANCAMENTOSFRONT L,
    MOVIMENTOHOSPEDES MV,
    PESSOA P,
    RESERVASFRONT R,
    CONTASFRONT C,
    CABNOTA CN,
    HOSPEDE H,
    TIPODEBCREDHOTEL T,
    PESSOA PESS,
    USUARIOSISTEMA U,
    PARAMHOTEL PH,
    NOTAELETRONICA NE,
    HISTNOTACANC HN,
    EMPRESAPROP E,
    MODELONFVHF MNF,
    TARIFAHOTEL T1,
    SEGMENTO SG,
    ORIGEMRESERVA O,
    meioscomunicacao M,
    ENDPESS EP,
    veiculoscomunica v,
    TIPODOCPESSOA TP,
    TIPOHOSPEDE TH
WHERE (R.STATUSRESERVA = 3)
    AND TH.IDTIPOHOSPEDE = MV.IDTIPOHOSPEDE
    and R.IDHOTEL = TH.IDHOTEL
    and TH.descricao <> 'Sem CashBack'
    AND (C.IDHOTEL = R.IDHOTEL(+))
    AND R.IDHOTEL = E.IDPESSOA
    and p.idpessoa = h.idhospede
    AND h.IDHOSPEDE = p.idpessoa
    AND r.CODSEGMENTO = SG.CODSEGMENTO
    AND R.IDHOTEL = SG.IDHOTEL
    AND r.IDORIGEM = O.IDORIGEM
    and r.idveiculos = v.idveiculos
    and r.idmeiocomunicacao = m.idmeiocomunicacao
    AND (C.IDFORCLI = PESS.IDPESSOA (+))
    AND (C.IDHOSPEDE = H.IDHOSPEDE(+))
    AND (C.IDRESERVASFRONT = R.IDRESERVASFRONT(+))
    AND (C.IDCONTA = CN.IDCONTA(+))
    AND (N.IDCONTA = L.IDCONTA (+))
    AND (N.IDHOTEL = L.IDHOTEL(+))
    and (n.IDNOTAFRONT = l.IDNOTAFRONT(+))
    AND (HN.IDLANCAMENTO(+) = L.IDLANCAMENTO)
    AND (HN.IDNOTAFRONT(+) = L.IDNOTAFRONT)
    AND (L.IDTIPODEBCRED = T.IDTIPODEBCRED(+))
    AND (N.IDUSUARIOEMISSAO = U.IDUSUARIO(+))
    AND (L.IDHOTEL = T.IDHOTEL(+))
    AND (N.CODMODELONFVHF(+) = MNF.CODMODELONFVHF)
    AND (N.IDNOTAFRONT = NE.IDNOTAFRONT(+))
    AND (N.IDHOTEL = C.IDHOTEL)
    AND (N.IDCONTA = C.IDCONTA)
    AND (N.IDHOTEL = PH.IDHOTEL)
    AND (R.IDHOTEL = PH.IDHOTEL)
    AND R.IDTARIFA = T1.IDTARIFA
    AND P.IDPESSOA = EP.IDPESSOA
    AND (C.IDHOSPEDE = MV.IDHOSPEDE)
    and r.idhotel = t1.idhotel
    AND P.IDDOCUMENTO = TP.IDDOCUMENTO
    AND R.DATAPARTIDAREAL = TO_DATE(TO_CHAR(SYSDATE - 1, 'DD/MM/YYYY'), 'DD/MM/YYYY')
GROUP BY MV.IDTIPOHOSPEDE,
    R.NUMRESERVA,
    R.CODREFERENCIA,
    r.DATARESERVA,
    r.datachegadareal,
    r.datapartidareal,
    SG.DESCRICAO,
    m.descricao,
    v.descricao,
    O.DESCRICAO,
    R.CODUH,
    T1.DESCRICAO,
    e.nomeempresa,
    P.Nome,
    p.email,
    TP.NOMEDOCUMENTO,
    p.numdocumento
ORDER BY R.NUMRESERVA,
    R.CODREFERENCIA,
    r.DATARESERVA,
    r.datachegadareal