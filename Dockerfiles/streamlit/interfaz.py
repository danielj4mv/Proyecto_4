import streamlit as st
import requests
import json

# Título de la página
st.title('Formulario para para prediccion de precios de propiedades')
st.subheader('Información del modelo usado')
st.json(requests.get("http://fastapi:8085/").json())

# Crear formulario con sus campos
with st.form(key='property_listing_form'):
    brokered_by = st.number_input('Brokered By', value=101640.0)
    status = st.text_input('Status', value='for_sale')
    bed = st.number_input('Number of Beds', value=4.0)
    bath = st.number_input('Number of Baths', value=2.0)
    acre_lot = st.number_input('Acre Lot', value=0.38)
    street = st.number_input('Street Number', value=1758218.0)
    state = st.text_input('State', value='Massachusetts')
    zip_code = st.number_input('ZIP Code', value=6016.0)
    house_size = st.number_input('House Size (sqft)', value=1617.0)

    submit_button = st.form_submit_button(label='Submit')

# Acción a realizar al enviar el formulario
if submit_button:
    data = {
        "brokered_by": brokered_by,
        "status": status,
        "bed": bed,
        "bath": bath,
        "acre_lot": acre_lot,
        "street": street,
        "state": state,
        "zip_code": zip_code,
        "house_size": house_size
    }

    # Mostrar los datos como JSON en la aplicación
    st.json(data)

    # Generar predición
    response = requests.post("http://fastapi:8085/predict", json=data)
    st.subheader('Predicción de precios del propiedades inmobiliarias')
    st.json(response.json())
