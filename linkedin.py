# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
import altair as alt
import pandas as pd



# Titre principal de l'application
st.title("Tableau de bord LinkedIn : Analyse des données")
st.write(
    """
    Ce tableau de bord présente une analyse globale du marché de l’emploi sur LinkedIn, en mettant en évidence les postes les plus fréquemment publiés et les mieux rémunérés par industrie, ainsi que la répartition des offres selon la taille des entreprises, les secteurs d’activité et les types de contrats proposés.
    """
)

# Connexion à Snowflake
session = get_active_session()

# Requête SQL1: pour extraire les 10 postes les mieux rémunérés par industrie
# Titre Streamlit
st.title("📌 Top 10 des titres de postes les plus publiés par industrie")

query = """
    SELECT 
        ji.industry_id,
        jp.title,
        COUNT(*) AS post_count
    FROM raw.job_postings jp
    JOIN raw.job_industries ji ON jp.job_id = ji.job_id
    GROUP BY ji.industry_id, jp.title
    ORDER BY post_count DESC
    LIMIT 10
"""

# Exécution et transformation en DataFrame pandas
df = session.sql(query).to_pandas()

# ✅ Affichage tableau
st.subheader("🗃️ Données brutes")
st.dataframe(df.rename(columns={
    "INDUSTRY_ID": "Secteur",
    "TITLE": "Titre du poste",
    "POST_COUNT": "Nombre d’offres"
}))

# ✅ Affichage graphique avec st.bar_chart
st.subheader("📈 Diagramme interactif (st.bar_chart)")
# Regrouper les titres avec leurs compteurs
bar_df = df[["TITLE", "POST_COUNT"]].set_index("TITLE")
st.bar_chart(bar_df)


# Requête SQL 2: Top 10 des postes les mieux rémunérés par industrie.:

# 📦 Requête SQL pour récupérer les données
st.title("💰 Top 10 des postes les mieux rémunérés par industrie")

# Connexion à la session Snowpark
session = get_active_session()
# Requête SQL : top salaires par poste et industrie
result = session.sql("""
     SELECT 
        ji.industry_id,
        jp.title,
        MAX(jp.max_salary) AS max_salary
    FROM raw.job_postings jp
    JOIN raw.job_industries ji ON jp.job_id = ji.job_id
    WHERE jp.max_salary IS NOT NULL
    GROUP BY ji.industry_id, jp.title
    ORDER BY max_salary DESC
    LIMIT 10
""").collect()

# Chargement dans pandas
df = pd.DataFrame(result)
df.columns = df.columns.str.lower()

# Graphique Altair : barres horizontales
chart = alt.Chart(df).mark_bar(cornerRadius=4).encode(
    x=alt.X('max_salary:Q', title="Salaire maximum (USD)"),
    y=alt.Y('title:N', sort='-x', title="Poste"),
    color=alt.Color('industry_id:N', title="Industrie"),
    tooltip=['title', 'industry_id', 'max_salary']
).properties(
    width='container',
    height=400
)

# Ajout des valeurs
text = chart.mark_text(
    align='left',
    baseline='middle',
    dx=3
).encode(
    text='max_salary:Q'
)

# Affichage final
st.altair_chart(chart + text, use_container_width=True)



# 🧠 Requête SQL 3: Répartition des offres par taille d'entreprise
# Résultat récupéré depuis Snowpark
# Titre
st.title("Répartition des offres d’emploi par taille d’entreprise.")

result = session.sql("""
    SELECT 
        c.name AS company_name,
        c.company_size,
        COUNT(*) AS nb_offres
    FROM raw.job_postings jp
    JOIN raw.companies c 
        ON SPLIT_PART(jp.company_id, '.', 1) = c.company_id
    WHERE c.company_size IS NOT NULL
    GROUP BY c.name, c.company_size
    ORDER BY nb_offres DESC
""").collect()

# Chargement dans pandas
df = pd.DataFrame(result)
df.columns = df.columns.str.lower()

# Liste unique des tailles d’entreprise
available_sizes = df['company_size'].unique().tolist()

# Sélecteur interactif
selected_sizes = st.multiselect(
    "Filtrer par taille d'entreprise :",
    options=available_sizes,
    default=available_sizes
)

# Filtrage des données selon la sélection
filtered_df = df[df['company_size'].isin(selected_sizes)]

st.caption("Filtrable par taille d'entreprise")

# Graphique Altair : barres verticales groupées
chart = alt.Chart(filtered_df).mark_bar().encode(
    x=alt.X('company_name:N', sort='-y', title="Entreprise"),
    y=alt.Y('nb_offres:Q', title="Nombre d'offres"),
    color=alt.Color('company_size:N', title="Taille d'entreprise"),
    tooltip=['company_name', 'company_size', 'nb_offres']
).properties(
    width=700,
    height=400
).configure_axisX(labelAngle=-45)

# Affichage du graphique
st.altair_chart(chart, use_container_width=True)



# Requête SQL4: pour obtenir la répartition des offres par secteur (industry_id)
result = session.sql("""
    SELECT 
        ji.industry_id,
        COUNT(*) AS nb_offres
    FROM raw.job_postings jp
    JOIN raw.job_industries ji ON jp.job_id = ji.job_id
    WHERE ji.industry_id IS NOT NULL
    GROUP BY ji.industry_id
    ORDER BY nb_offres DESC
    LIMIT 20
""").collect()
# Chargement dans pandas
df = pd.DataFrame(result)
df.columns = df.columns.str.lower()

# Titre
st.title("📊 Répartition des offres d’emploi par secteur d’activité")

# Graphique à barres horizontales
chart = alt.Chart(df).mark_bar(cornerRadius=3).encode(
    x=alt.X('nb_offres:Q', title='Nombre d’offres'),
    y=alt.Y('industry_id:N', sort='-x', title='Secteur (industry_id)'),
    tooltip=['industry_id', 'nb_offres']
).properties(
    width='container',
    height=500
)

# Ajout des étiquettes de valeurs
text = chart.mark_text(
    align='left',
    baseline='middle',
    dx=3
).encode(
    text='nb_offres:Q'
)

# Affichage
st.altair_chart(chart + text, use_container_width=True)

#requete5:Répartition des offres d’emploi par type d’emploi (temps plein, stage, temps partiel).

# Titre
st.title("🥧 Répartition des offres d’emploi par type d’emploi")

result = session.sql("""
     SELECT work_type, COUNT(*) AS nb_offres
    FROM raw.job_postings
    WHERE work_type IS NOT NULL
    GROUP BY work_type
""").collect()

# Conversion en DataFrame et nettoyage noms colonnes
df = pd.DataFrame(result)
df.columns = df.columns.str.lower()

# Calcul pourcentage
df['pct'] = df['nb_offres'] / df['nb_offres'].sum()

# Création du camembert Altair (donut)
pie = alt.Chart(df).mark_arc(innerRadius=60).encode(
    theta=alt.Theta(field="nb_offres", type="quantitative"),
    color=alt.Color(field="work_type", type="nominal", legend=alt.Legend(title="Type d’emploi")),
    tooltip=[alt.Tooltip('work_type:N', title='Type'), alt.Tooltip('nb_offres:Q', title='Nombre d\'offres'), alt.Tooltip('pct:Q', format='.1%', title='Pourcentage')]
)

# Ajout des labels à l’extérieur, lisibles horizontalement
labels = alt.Chart(df).mark_text(radius=140, size=10).encode(
    theta=alt.Theta(field="nb_offres", type="quantitative"),
    text=alt.Text('work_type:N')
)

st.altair_chart(pie + labels, use_container_width=True)




