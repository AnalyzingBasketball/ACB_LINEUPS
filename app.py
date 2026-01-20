def get_available_games():
    # Escribimos la dirección FIJA para evitar errores de variables
    # Asegúrate de que tu dataset se llama 'acb_data' y la tabla 'lineups'
    query = """
        SELECT DISTINCT GameID, Season, Week, Team, Location 
        FROM `acb-lineups.acb_data.lineups`
        ORDER BY GameID DESC
    """
    try:
        return client.query(query).to_dataframe()
    except Exception as e:
        # ESTO ES LO IMPORTANTE: Nos imprimirá el error real en la web
        st.error(f"🚨 ERROR EN LA CONSULTA SQL: {e}")
        st.stop()
        return pd.DataFrame() # Return vacío para que no explote
