-- Vue pour le suivi des livraisons
CREATE OR REPLACE VIEW livraison AS (
    -- Le contenu de cette vue sera géré par dbt dans dim_livraison.sql
);

-- Vue pour les informations logistiques
CREATE OR REPLACE VIEW logistique AS (
    SELECT 
        p.productvendor,
        p.productname,
        p.productline,
        p.quantityinstock,
        p.buyprice,
        od.ordernumber,
        od.quantityordered,
        o.orderdate,
        od.quantityordered * p.buyprice AS prix_achat,
        p.quantityinstock * p.buyprice AS prix_stock
    FROM 
        PRODUCTS p
    LEFT JOIN 
        ORDERDETAILS od ON od.productCode = p.productCode
    LEFT JOIN 
        ORDERS o ON o.ordernumber = od.ordernumber
);

-- Les autres vues (Status_stock, vente, payment_client, Ressources_Humaines) 
-- seront également gérées par dbt dans des fichiers séparés