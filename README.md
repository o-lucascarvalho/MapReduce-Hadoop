# MapReduce-Hadoop
Trabalho avaliativo para matéria de Frameworks para Big Data do curso de pós graduação em Data Science pela PUCPR. 
Atividade consiste na implementação de rotinas de Map Reduce para responder aos seguintes items. 


1. O número de transações por país; 
2. O número de transações por tipo de fluxo e por ano; 
3. O  preço  médio  das  commodities  por  tipo  de  fluxo  e  ano.  Importante:  o  preço  de  cada commodity deve ser calculado ao dividir price por unit; 
4. O preço médio das transações que envolvem o Brasil por tipo de unidade e fluxo; 
5. A commodity mais comercializada (ao somar as quantidades) em 2016, por tipo de fluxo. 
Importante:  para  resolver  este  exercício,  será  necessário  implementar  duas  rotinas 
MapReduce); 

Exemplo do arquivo CSV utilizado.
+-------------------+----+---------+--------------------+---------+---------+----------+-------------------+----------+--------------------+
|    country_or_area|year|comm_code|           commodity|     flow|trade_usd| weight_kg|      quantity_name|  quantity|            category|
+-------------------+----+---------+--------------------+---------+---------+----------+-------------------+----------+--------------------+
|            Belgium|2016|   920510|Brass-wind instru...|   Export|   571297|    3966.0|    Number of items|    4135.0|92_musical_instru...|
|          Guatemala|2008|   660200|Walking-sticks, s...|   Export|    35022|    5575.0|    Number of items|   10089.0|66_umbrellas_walk...|
|           Barbados|2006|   220210|Beverage waters, ...|Re-Export|    81058|   44458.0|   Volume in litres|   24113.0|22_beverages_spir...|
|            Tunisia|2016|   780411|Lead foil of a th...|   Import|     4658|     121.0|Weight in kilograms|     121.0|78_lead_and_artic...|
|          Lithuania|1996|   560110|Sanitary towels, ...|   Export|    76499|    5419.0|Weight in kilograms|    5419.0|56_wadding_felt_n...|
|            Denmark|2011|   310100|Animal or vegetab...|   Export|  4903675|1.902844E7|Weight in kilograms|1.902844E7|      31_fertilizers|
|           Thailand|1994|   920290|String musical in...|   Import|  2088672|       0.0|    Number of items|   59595.0|92_musical_instru...|
|           Portugal|2004|   511119|Woven fabric, >85...|   Export|  1546575|   87367.0|Weight in kilograms|   87367.0|51_wool_animal_ha...|
|              Congo|2011|   420690|Articles of gut, ...|   Export|      883|       9.0|Weight in kilograms|       9.0|42_articles_of_le...|
|Antigua and Barbuda|2016|   620332|Mens, boys jacket...|   Export|    12988|    1403.0|    Number of items|     648.0|62_articles_of_ap...|
+-------------------+----+---------+--------------------+---------+---------+----------+-------------------+----------+--------------------+
