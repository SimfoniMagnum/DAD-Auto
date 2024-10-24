from src.config import Config
from src.db_utils import DB_Utils
from src.common_utils import columns_input, save_file, if_exists
from src.components.Opportunity_Calculation.supplier_consolidation import Supplier_Consolidation
from src.components.Opportunity_Calculation.supplier_normalization import Supplier_Normalization
from src.components.Opportunity_Calculation.supplier_commanality import Supplier_Commanality
from src.components.Opportunity_Calculation.vendor_opportunity import Vendor_Opportunity
from src.components.Opportunity_Calculation.contract_compliance import Contract_Compliance
from src.components.Opportunity_Calculation.po_compliance import PO_Compliance
from src.components.flag_mapping.map_supplier_consolidation import Map_Supplier_Consolidation
from src.components.flag_mapping.map_supplier_normalization import Map_Supplier_Normalization
from src.components.flag_mapping.map_supplier_commanality import Map_Supplier_Commanality
from src.components.flag_mapping.map_vendor_opportunity import Map_Vendor_Opportunity
from src.components.flag_mapping.map_contract_compliance import Map_Contract_Compliance
from src.components.flag_mapping.map_po_compliance import Map_PO_Compliance
from src.components.Opportunity_Calculation.payment_days import Payment_Days
from src.components.flag_mapping.map_payment_days import Map_Payment_Days
from src.components.Opportunity_Calculation.catalouging_opportunity import Cataloguing_Opportunity
from src.components.flag_mapping.map_catalouging_opportunity import Map_Catalouging_Opportunity
from src.components.Opportunity_Calculation.payment_terms_calculation import Payment_Terms_Calculation
from src.components.flag_mapping.payment_term_mapping import Payment_Term_Mapping
from src.fetch_input_file_details import Fetch_configInputs
from src import log_utils
import pandas as pd

pd.set_option('display.max_columns', None)
import warnings

warnings.filterwarnings('ignore');
import os
import pickle
from dotenv import load_dotenv

load_dotenv()
import streamlit as st
import pandas as pd
from src.config import Config
from src.db_utils import DB_Utils
import os
from dotenv import load_dotenv
from datetime import datetime
import time

load_dotenv()

st.set_page_config(
    page_title="Simfoni Opportunity Assessment",
    page_icon="📊",  # You can also use a local file like 'favicon.png'
    layout="wide",  # You can use 'centered' if you prefer a narrower layout
    initial_sidebar_state="expanded"  # Sidebar can be 'expanded' or 'collapsed'
)


def establish_connection():
    with st.spinner("Processing...."):
        try:
            db_utils = DB_Utils(st.session_state['credentials'], st.session_state['db_table_name'])

            db_utils.connection = db_utils.establish_connection()
            db_utils.cursor = db_utils.establish_cursor()
            if db_utils and db_utils.connection and db_utils.cursor:
                st.success('Connected To Database Successfully.')
                st.session_state['db_utils'] = db_utils
                st.session_state['connection'] = db_utils.connection
                st.session_state['cursor'] = db_utils.cursor
                return True
            else:
                return False
        except Exception as e:
            st.error(f'Unexpected {e} Error Occured, Please Check. inside Establish Connection Function')


def fetch_table_columns():
    #: when connection with database is established successfully
    if establish_connection():
        try:
            with st.spinner("Processing...."):
                st.session_state['db_utils'].db_columns = st.session_state['db_utils'].retrieve_db_cols()
                if len(st.session_state['db_utils'].db_columns) != 0:
                    print("column names fetched successfully")
                    st.success('Column Names Fetched Successfully from database. ')
                    st.session_state['db_columns'] = st.session_state['db_utils'].db_columns
                    st.session_state['db_connected'] = True
                    # print("Following are db_columns: ", {st.session_state['db_columns']})
                    return True
                #: if columns are not fetched successfully from the database
                else:
                    st.session_state['db_columns'] = None
                    st.session_state['db_connected'] = False
                    return False

        except Exception as e:
            st.warning(f'Unexpected {e} Error Occured Inside Fetch Table Function')
            st.session_state['db_columns'] = None
            st.session_state['db_connected'] = False
            #: when the connection is not established successfully with database
    else:
        st.warning("Unable To Established Connection with Database, Please Try After Some time")


def fetch_distinct_values():
    with st.spinner("Processing.."):
        try:
            if st.session_state['db_utils'].connection:
                if st.session_state['db_utils'].cursor:
                    st.success("Connecting with database")
                    category_level_3_col = st.session_state['data']['DBT_Category_Level_3_Column'][0]
                    #: ---------------------fetching distinct category level 3----------------------------------------------
                    query_2 = f'''select distinct "{category_level_3_col}" from "{st.session_state['db_table_name']}" '''
                    st.session_state['db_utils'].cursor.execute(query_2)
                    result_2 = st.session_state['db_utils'].cursor.fetchall()
                    distinct_cats = [item[0] for item in result_2]
                    st.session_state['Distinct_category_level_3'] = distinct_cats

                    #: ----------------------------------fetching distinct data sources----------------------------------
                    data_source_col = st.session_state['data']['DBT_Data_Source_Column'][0]
                    query_1 = f''' select distinct "{data_source_col}" from "{st.session_state['db_table_name']}" '''
                    st.session_state['db_utils'].cursor.execute(query_1)
                    result_1 = st.session_state['db_utils'].cursor.fetchall()
                    distinct_ds = [item[0] for item in result_1]
                    st.session_state['distinct_data_sources'] = distinct_ds

                    if st.session_state['data']['DBT_Contract_Flag_Available'][0] == 'Yes':
                        #: ---------------------------------- distinct contract flag:----------------------------------
                        contract_flag_col = st.session_state['data']['DBT_Contract_Flag_Column'][0]
                        query_5 = f'''select distinct "{contract_flag_col}" from "{st.session_state['db_table_name']}" '''
                        st.session_state['db_utils'].cursor.execute(query_5)
                        result_5 = st.session_state['db_utils'].cursor.fetchall()
                        contract_flags = [item[0] for item in result_5]
                        st.session_state['Distinct_contract_flag'] = contract_flags

                    if st.session_state['data']['DBT_PO_Flag_Available'][0] == 'Yes':
                        #: ----------------------------------distinct po flag values:----------------------------------
                        po_flag_col = st.session_state['data']['DBT_PO_Flag_Column'][0]
                        query_6 = f'''select distinct "{po_flag_col}" from "{st.session_state['db_table_name']}" '''
                        st.session_state['db_utils'].cursor.execute(query_6)
                        result_6 = st.session_state['db_utils'].cursor.fetchall()
                        po_flags = [item[0] for item in result_6]
                        st.session_state['Distinct_po_flag'] = po_flags

                    #: ----------------------------------fetching distinct value of extra exclusion column ----------------------------------
                    if st.session_state['data']['DBT_Extra_Filter_Available'][0] == 'Yes':
                        extra_filter_col_name = st.session_state['data']['DBT_Extra_Filter_Column']
                        query_3 = f''' select distinct "{extra_filter_col_name}" from "{st.session_state['db_table_name']}" '''
                        st.session_state['db_utils'].cursor.execute(query_3)
                        result_3 = st.session_state['db_utils'].cursor.fetchall()
                        distinct_extra_exclusion_cols_flags = [item[0] for item in result_3]
                        st.session_state['Distinct_exclusion_flags'] = distinct_extra_exclusion_cols_flags

                    if st.session_state['data']['DBT_InScope_Filter_Available'][0] == 'Yes':
                        #: ----------------------------------distinct value from scope column----------------------------------
                        inscope_column = st.session_state['data']['DBT_InScope_Column'][0]
                        query_4 = f''' select distinct "{inscope_column}" from "{st.session_state['db_table_name']}" '''
                        st.session_state['db_utils'].cursor.execute(query_4)
                        result_4 = st.session_state['db_utils'].cursor.fetchall()
                        inscope_flags = [item[0] for item in result_4]
                        st.session_state['Distinct_inscope'] = inscope_flags
                    return True
            else:
                st.error("Failed to establish Connection with database Fetch Distinct Values ")
                return False

        except Exception as e:
            if st.session_state['db_utils'].connection:
                st.session_state['db_utils'].connection.rollback()
            st.error(f'Unexpected {e} Error Occured Fetch Distinct Values ')
            return False


class InScoping_App:
    def __init__(self):
        pass

    @st.experimental_dialog("Fetching ")
    def show_processing_dialog(self):
        st.write("Please wait while we fetch details from database")
        flag = fetch_table_columns()
        if flag:
            st.success('Now, Press Next To Fill The InScoping Form')

    def input(self):
        try:
            st.title('Connection To Simfoni DataBase ')
            db_choice = st.selectbox("Select The Database Name: ", ['SADB', 'VIRTUOSI'], index=None,
                                     placeholder='Select A Simfoni Database.',
                                     help="Select the database which has your client's data")
            if db_choice == 'SADB':
                Config.credentials = {
                    'hostname': os.getenv('sadb_host'),
                    'database': os.getenv('sadb_database'),
                    'username': os.getenv('sadb_username'),
                    'password': os.getenv('sadb_password'),
                    'port': os.getenv('sadb_port')
                }
            elif db_choice == 'VIRTUOSI':
                Config.credentials = {
                    'hostname': os.getenv('virtuosi_host'),
                    'database': os.getenv('virtuosi_database'),
                    'username': os.getenv('virtuosi_username'),
                    'password': os.getenv('virtuosi_password'),
                    'port': os.getenv('virtuosi_port')}
            st.session_state['db_choice'] = db_choice
            table_name = st.text_input('Provide the Database Table Name: ', placeholder='Provide a table name.')
            st.session_state['db_table_name'] = table_name
            fetch_button = st.button('Connect To Database.', disabled=not (db_choice and table_name))
            if table_name and db_choice:
                st.session_state['credentials'] = Config.credentials
                st.session_state['db_table_name'] = table_name
                if fetch_button:
                    self.show_processing_dialog()
                    time.sleep(2)
            #: while returning from show processing fun we have set db_connected True, so we can go to inscope form
            if 'db_connected' in st.session_state and st.session_state['db_connected']:
                if st.button('Next'):
                    st.session_state['show_inscope_form'] = True
        except Exception as e:
            st.error(f"Unexpected {e} Error Occured Inside Input Function")

    def exclusion_form(self):
        try:
            st.title("Exclusion form")
            if 'Distinct_inscope' in st.session_state and st.session_state['Distinct_inscope'] is not None:
                inscope_keyword = st.selectbox('Select the Keyword for InScope Filter ',
                                               ['Select All'] + st.session_state['Distinct_inscope'])

            if 'Distinct_exclusion_flags' in st.session_state and st.session_state[
                'Distinct_exclusion_flags'] is not None:
                extra_exclusion_keyword = st.selectbox("Select the Keyword for Extra Exclusion Filter. ",
                                                       ['Select All'] + st.session_state['Distinct_exclusion_flags'])

            if 'Distinct_contract_flag' in st.session_state and st.session_state['Distinct_contract_flag'] is not None:
                on_contract_key = st.multiselect(
                    "Select the Keyword to be flagged as  'ON Contract Key' In Contract Compliance",
                    ['Select All'] + st.session_state['Distinct_contract_flag'])

            if 'Distinct_po_flag' in st.session_state and st.session_state['Distinct_po_flag'] is not None:
                po_key = st.multiselect("Select the Keyword to be flagged as 'PO' in PO Compliance",
                                        ['Select All'] + st.session_state['Distinct_po_flag'])

            if 'distinct_data_sources' in st.session_state and st.session_state['distinct_data_sources'] is not None:
                supplier_normalization_data_sources = st.multiselect(
                    'Select the data source/sources for Supplier Normalization ',
                    ['Select All'] + st.session_state['distinct_data_sources'])

            if 'distinct_data_sources' in st.session_state and st.session_state['distinct_data_sources'] is not None:
                supplier_commanality_data_sources = st.multiselect(
                    'Select the data source/sources for Supplier Commanality ',
                    ['Select All'] + st.session_state['distinct_data_sources'])

            if 'distinct_data_sources' in st.session_state and st.session_state['distinct_data_sources'] is not None:
                onetime_vendor_data_sources = st.multiselect('Select the data source/sources for One time Vendor ',
                                                             ['Select All'] + st.session_state['distinct_data_sources'])

            if 'distinct_data_sources' in st.session_state and st.session_state['distinct_data_sources'] is not None:
                contract_compliance_data_sources = st.multiselect(
                    'Select the data source/sources for Contract Compliance ',
                    ['Select All'] + st.session_state['distinct_data_sources'])

            if 'distinct_data_sources' in st.session_state and st.session_state['distinct_data_sources'] is not None:
                po_compliance_data_sources = st.multiselect('Select the data source/sources for PO Compliance ',
                                                            ['Select All'] + st.session_state['distinct_data_sources'])

            if 'distinct_data_sources' in st.session_state and st.session_state['distinct_data_sources'] is not None:
                catalouging_data_sources = st.multiselect('Select the data source/sources for Catalouging ',
                                                          ['Select All'] + st.session_state['distinct_data_sources'])

            if 'Distinct_category_level_3' in st.session_state and st.session_state[
                'Distinct_category_level_3'] is not None:
                supplier_consolidation_level_3_exclusion = st.multiselect(
                    "Select the Categorires to be excluded from Supplier Consolidation ",
                    [' '] + st.session_state['Distinct_category_level_3'])

            uploaded_file = st.file_uploader('Upload the File Which Has Categories To Be Selected For Cataloguing. ',
                                             type=['xlsx', 'xls'])
            if uploaded_file:
                cataloguing_df = pd.read_excel(uploaded_file)
                column = st.selectbox('Select the Name of Category Column', cataloguing_df.columns, index=None,
                                      placeholder='Category Column Name')
                if column:
                    catalouging_level_3_categories = list(cataloguing_df[column].unique())

            uploaded_file2 = st.file_uploader("Provide the Payment Terms File For Mapping Payment Days. ",
                                              type=['xlsx', 'xls'])
            if uploaded_file2:
                directory = "./Inscoping Input Excel"
                payment_term_df = pd.read_excel(uploaded_file2)
                payment_term_file_name = os.path.join(directory, f'{uploaded_file2.name}')
                payment_term_df.to_excel(payment_term_file_name, index=False)
                payment_terms_codes = st.selectbox("Select the Payment Terms Code Column", payment_term_df.columns)
                payment_terms_description = st.selectbox("Select the Payment Terms Description Column",
                                                         payment_term_df.columns)
                payments_days = st.selectbox('Select the Payment Days Column', payment_term_df.columns)

            col1, col2 = st.columns([5, 1])
            with col1:
                if st.button("Proceed"):
                    if 'Distinct_inscope' in st.session_state and not inscope_keyword:
                        st.warning("Please Provide InScope Keyword Value")

                    elif 'Distinct_exclusion_flags' in st.session_state and not extra_exclusion_keyword:
                        st.warning("Please provide Extra Exclusion Column Keyword Value")

                    elif 'Distinct_contract_flag' in st.session_state and not on_contract_key:
                        st.warning("Please Provide Flag For ON Contract In Contract Compliance")

                    elif 'Distinct_po_flag' in st.session_state and not po_key:
                        st.warning("Please Provide Flag For PO In PO Compliance")

                    elif 'Distinct_category_level_3' in st.session_state and not catalouging_level_3_categories:
                        st.warning('Please select Categories for Catalouging')

                    elif 'distinct_data_sources' in st.session_state and (
                            not supplier_normalization_data_sources or not supplier_commanality_data_sources or not onetime_vendor_data_sources or not contract_compliance_data_sources or not po_compliance_data_sources or not catalouging_data_sources):
                        st.warning('Please Select Data Source')
                    elif not uploaded_file2 and (
                            not payment_terms_codes or not payment_terms_description or not payments_days):
                        st.warning("Provide all the details Regarding Payment Terms")
                    else:
                        st.session_state['data'].update({
                            'DBT_inscope_keyword': [
                                inscope_keyword if 'Distinct_inscope' in st.session_state else 'Not Available'],
                            'DBT_Extra_exclusion_keyword': [
                                extra_exclusion_keyword if 'Distinct_exclusion_flags' in st.session_state else 'Not Available'],
                            'DBT_ON_contract_key': [
                                on_contract_key if 'Distinct_contract_flag' in st.session_state else 'Not Available'],
                            'DBT_PO_key': [po_key if 'Distinct_po_flag' in st.session_state else 'Not Available'],
                            'DBT_Supplier_normalization_data_sources': [
                                supplier_normalization_data_sources if 'distinct_data_sources' in st.session_state else 'Not Available'],
                            'DBT_Supplier_commanality_data_sources': [
                                supplier_commanality_data_sources if 'distinct_data_sources' in st.session_state else 'Not Available'],
                            'DBT_Onetime_vendor_data_sources': [
                                onetime_vendor_data_sources if 'distinct_data_sources' in st.session_state else 'Not Available'],
                            'DBT_Contract_compliance_data_sources': [
                                contract_compliance_data_sources if 'distinct_data_sources' in st.session_state else 'Not Available'],
                            'DBT_PO_compliance_data_sources': [
                                po_compliance_data_sources if 'distinct_data_sources' in st.session_state else 'Not Available'],
                            'DBT_Catalouging_data_sources': [
                                catalouging_data_sources if 'distinct_data_sources' in st.session_state else 'Not Available'],
                            'DBT_Catalouging_level_3_categories': [
                                catalouging_level_3_categories if 'Distinct_category_level_3' in st.session_state else 'Not Available'],
                            'DBT_Supplier_consolidation_level_3_Exclusion': [
                                supplier_consolidation_level_3_exclusion if 'Distinct_category_level_3' in st.session_state else 'NoExclusion'],
                            'PTF_payment_terms_code': [payment_terms_codes],
                            'PTF_payment_terms_description': [payment_terms_description],
                            'PTF_payments_days': [payments_days],
                            'PTF_payment_term_file_path': [payment_term_file_name]
                        })

                        directory = "./Inscoping Input Excel"
                        os.makedirs(directory, exist_ok=True)
                        df = pd.DataFrame(st.session_state['data'])
                        file_name = os.path.join(directory, f"{st.session_state['db_table_name']}.xlsx")
                        df.to_excel(file_name, index=False)
                        st.success(
                            f'InScoping  Data Loaded Successfully In {file_name} and saved in Current Working Directory')
                        st.session_state.input_file_path = file_name
            with col2:
                if st.button('Close'):
                    if st.session_state['db_utils'].connection:
                        st.session_state['db_utils'].connection.close()
                        print("Connection Closed Succesfully")
        except Exception as e:
            st.warning(f'Unexpected {e} Error Occured')

    def inscope_form(self):
        st.title('Inscoping Form')
        client_name = st.text_input("Client Name")
        opportunity_assessment_table = st.text_input("Opportunity Assessment Table Name",
                                                     placeholder='Name to create Opportunity Assessment Table')

        start_date = st.date_input("Start Date", value=datetime.today(), format="YYYY/MM/DD",
                                   help='Date From Which We Have to Extract Data For OA!')
        end_date = st.date_input("End Date", value=datetime.today(), format="YYYY/MM/DD",
                                 help='Date Till Which We Have to Extract Data For OA!')

        inscope_filter = st.radio("Is InScope Filter Available In Data?", ('Yes', 'No'))
        if inscope_filter == 'Yes':
            inscope_column = st.selectbox('Select the Scope Column Name? ', [" "] + st.session_state['db_columns'])

        extra_filter = st.radio('Do We Have An Extra Filter In Data?', ("Yes", 'No'))
        extra_filter_column = ''
        if extra_filter == 'Yes':
            extra_filter_column = st.selectbox("Column Name Which Has That Extra Filter ",
                                               [" "] + st.session_state['db_columns'])

        # Selecting columns
        data_source_col = st.selectbox('Select the Data Source Column', [" "] + st.session_state['db_columns'])
        supplier_name_col = st.selectbox("Select the Supplier Name Column", [" "] + st.session_state['db_columns'])
        supplier_name_normalized_col = st.selectbox('Select the Supplier Name (Normalized) Column',
                                                    [" "] + st.session_state['db_columns'])
        category_level_3_col = st.selectbox('Select the Category Level 3 Column',
                                            [" "] + st.session_state['db_columns'])
        document_number_col = st.selectbox('Select the Document Number Column', [" "] + st.session_state['db_columns'])
        date_col = st.selectbox('Select the Date Column', [" "] + st.session_state['db_columns'])
        spend_col = st.selectbox('Select The Spend Column', [" "] + st.session_state['db_columns'])
        serial_no_col = st.selectbox('Select the Serial No Column', [" "] + st.session_state['db_columns'])
        material_description_col = st.selectbox('Select the Material Description Column',
                                                [" "] + st.session_state['db_columns'])
        commonality_calculation_col = st.selectbox('Column Name To Be Used For Commonality Calculation',
                                                   [" "] + st.session_state['db_columns'])
        payment_terms_code_col = st.selectbox('Select the Payment Terms Code Column',
                                              [" "] + st.session_state['db_columns'])
        payment_terms_description_col = st.selectbox('Select the Payment Terms Description Column',
                                                     [" "] + st.session_state['db_columns'])

        contract_available = st.radio("Is Contract Information Available In Data?", ('Yes', 'No'))
        contract_flag_available = 'No'
        contract_number_available = 'No'
        if contract_available == 'Yes':
            contract_option = st.radio('Select the Contract Information Which Is Available?',
                                       ('Contract Flag', 'Contract Number'))
            if contract_option == 'Contract Flag':
                contract_flag_col = st.selectbox("Select the Contract Flag Column",
                                                 [" "] + st.session_state['db_columns'])
                contract_flag_available = 'Yes'
            elif contract_option == 'Contract Number':
                contract_no_col = st.selectbox("Select the Contract Number Column",
                                               [" "] + st.session_state['db_columns'])
                contract_number_available = 'Yes'

        po_available = st.radio("Do We Have PO Information Available In Data?", ('Yes', 'No'))
        po_flag_available = 'No'
        po_number_available = 'No'
        if po_available == 'Yes':
            po_option = st.radio('Select the PO Information Which Is Available?', ('PO Flag', 'PO Number'))
            if po_option == 'PO Flag':
                po_flag_col = st.selectbox("Select the PO Flag Column", [" "] + st.session_state['db_columns'])
                st.session_state['po_flag_col'] = po_flag_col
                po_flag_available = 'Yes'
            elif po_option == 'PO Number':
                po_no_col = st.selectbox("Select the PO Number Column", [" "] + st.session_state['db_columns'])
                st.session_state['po_no_col'] = po_no_col
                po_number_available = 'Yes'

        payment_term_mapping_key = st.selectbox(
            "Select the column name which we can use for Marking Payment Terms as Single/Multiple?",
            [" "] + st.session_state['db_columns'])
        if st.button("Submit"):
            # Validation checks
            if not client_name:
                st.warning('Please provide the Client Name.')
            elif not payment_term_mapping_key:
                st.warning("Please provide key to Mark Single/Multiple In Payment Terms")
            elif not opportunity_assessment_table:
                st.warning('Please provide Opportunity Assessment Table Name.')
            elif inscope_filter == 'Yes' and not inscope_column:
                st.warning("Please provide the Inscope Flag/Keyword value.")
            elif extra_filter == 'Yes' and not extra_filter_column:
                st.warning("Please provide the extra filter column name.")
            elif not data_source_col or data_source_col == " ":
                st.warning("Please select a Data Source Column.")
            elif not supplier_name_col or supplier_name_col == " ":
                st.warning("Please select a Supplier Name Column.")
            elif not supplier_name_normalized_col or supplier_name_normalized_col == " ":
                st.warning("Please select a Supplier Name (Normalized) Column.")
            elif not category_level_3_col or category_level_3_col == " ":
                st.warning("Please select a Category Level 3 Column.")
            elif not document_number_col or document_number_col == " ":
                st.warning("Please select a Document Number Column.")
            elif not date_col or date_col == " ":
                st.warning("Please select a Date Column.")
            elif not spend_col or spend_col == " ":
                st.warning("Please select a Spend Column.")
            elif not serial_no_col or serial_no_col == " ":
                st.warning("Please select a Serial No Column.")
            elif not material_description_col or material_description_col == " ":
                st.warning("Please select a Material Description Column.")
            elif not commonality_calculation_col or commonality_calculation_col == " ":
                st.warning("Please select a Column for Commonality Calculation.")
            elif not payment_terms_code_col or payment_terms_code_col == " ":
                st.warning("Please select a Payment Terms Code Column.")
            elif not payment_terms_description_col or payment_terms_description_col == " ":
                st.warning("Please select a Payment Terms Description Column.")

            else:
                data = {
                    'DBT_Database_Name': st.session_state['db_choice'],
                    'DBT_Client_Name': [client_name],
                    'DBT_Opportunity_Assessment_Table_Name': [opportunity_assessment_table],
                    'DBT_Start_Date': [start_date],
                    'DBT_End_Date': [end_date],
                    'DBT_InScope_Filter_Available': [inscope_filter],
                    'DBT_InScope_Column': [inscope_column if inscope_filter == 'Yes' else 'Not Available'],
                    'DBT_Extra_Filter_Available': [extra_filter],
                    'DBT_Extra_Filter_Column': [extra_filter_column if extra_filter == 'Yes' else 'Not Available'],
                    'DBT_Supplier_Name_Column': [supplier_name_col],
                    'DBT_Material_Description_Column': [material_description_col],
                    'DBT_Supplier_Name_(Normalized)_Column': [supplier_name_normalized_col],
                    'DBT_Spend_Column': [spend_col],
                    'DBT_Document_Number_Column': [document_number_col],
                    'DBT_Serial_Number_Column': [serial_no_col],
                    'DBT_Category_Level_3_Column': [category_level_3_col],  # Removed brackets
                    'DBT_Date_Column': [date_col],  # Removed brackets
                    'DBT_Data_Source_Column': [data_source_col],  # Removed brackets
                    'DBT_Payment_Terms_Code_Column': [payment_terms_code_col],  # Removed brackets
                    'DBT_Payment_Terms_Description_Column': [payment_terms_description_col],  # Removed brackets
                    'DBT_Contract_Information_Available': [contract_available],  # Removed brackets
                    'DBT_Contract_Flag_Available': [contract_flag_available],  # Removed brackets
                    'DBT_Contract_Number_Available': [contract_number_available],  # Removed brackets
                    'DBT_Contract_Flag_Column': [
                        contract_flag_col if contract_flag_available == 'Yes' else 'Not Available'],
                    # Removed brackets
                    'DBT_Contract_Number_Column': [
                        contract_no_col if contract_number_available == 'Yes' else 'Not Available'],
                    # Removed brackets
                    'DBT_PO_Flag_Column': [po_flag_col if po_flag_available == 'Yes' else 'Not Available'],
                    # Removed brackets
                    'DBT_PO_Number_Column': [po_no_col if po_number_available == 'Yes' else 'Not Available'],
                    # Removed brackets
                    'DBT_Commonality_Calculation_Column': [commonality_calculation_col],  # Removed brackets
                    'DBT_PO_Information_Available': [po_available],  # Removed brackets
                    'DBT_PO_Flag_Available': [po_flag_available],  # Removed brackets
                    'DBT_PO_Number_Available': [po_number_available],  # Removed brackets,
                    'DBT_Payment_Term_Mapping_Column': [payment_term_mapping_key]
                }

                st.session_state['data'] = data
                self.show_distinct_value_fetch_dialog()

    @st.experimental_dialog("Processing...")
    def show_distinct_value_fetch_dialog(self):
        st.write("Please Wait..")
        flag = fetch_distinct_values()
        if flag:
            st.success("Data regarding Exclusion fetched successfully")
            st.session_state['exclusion_form'] = True
            st.session_state['show_inscope_form'] = False


class Home_Page:
    def home_page(self):
        st.title('Welcome to Simfoni Opportunity Assessment Module')

        # Introduction to the app
        st.write("""
                Simfoni's internal Opportunity Assessment (OA) app is designed to help clients optimize their procurement and supplier 
                management processes, providing clear insights into their spend and identifying opportunities for cost savings. This tool 
                offers a comprehensive set of assessments that enable organizations to unlock hidden value in their procurement operations.
            """)

        # Key Features section
        st.subheader("Key Features of the Simfoni Opportunity Assessment App")

        # List of key features
        st.markdown("""
            - **Supplier Consolidation Opportunity**: Identify opportunities to consolidate suppliers, reducing fragmentation and improving efficiency in supplier management.
            - **One-Time Vendor Identification**: Spot one-time vendors and evaluate their impact on procurement processes, enabling better long-term supplier relationships.
            - **Supplier Normalization Opportunity**: Standardize supplier data to ensure consistency and accuracy across procurement operations, improving overall data integrity.
            - **Purchase Price Variance (PPV)**: Analyze and understand fluctuations in purchase prices to identify potential savings opportunities by managing vendor negotiations more effectively.
            - **Commonality**: Discover opportunities where commonly used items can be standardized across the organization to reduce costs and streamline procurement.
            - **Cataloguing and PO Compliance Flags**: Ensure that purchases comply with internal cataloguing standards and purchase order processes, leading to better governance and cost controls.
            - **Contract Compliance Flag**: Track and enforce contract compliance to ensure that procurement teams are adhering to negotiated terms and conditions.
            - **Payment Days**: Assess payment terms to optimize cash flow management and identify opportunities for better payment strategies.
            - **Single/Multiple Payment Term Opportunities**: Identify and evaluate opportunities to harmonize or optimize payment terms across multiple suppliers.
            """)

        # How it works section
        st.subheader("How It Works")

        # Explanation of the process
        st.write("""
            - **InScoping Form**: To begin the assessment, users must first complete the InScoping form, which captures key details about 
              the client’s procurement data. This step lays the foundation for a detailed opportunity analysis.

            - **Opportunity Calculations**: Once the form is submitted, the app calculates each opportunity one by one, analyzing the data 
              and updating the relevant flags in the database.

            - **Data-Driven Insights**: By analyzing and flagging areas like supplier consolidation, payment terms, and compliance, Simfoni’s 
              clients can gain a deeper understanding of their spend, helping them make more informed decisions.
            """)

        # Closing paragraph
        st.write("""
                This streamlined process not only improves procurement efficiency but also delivers measurable cost savings by providing 
                visibility into areas of opportunity. Simfoni’s Opportunity Assessment App helps clients realize the full potential of 
                their procurement data, ultimately driving business growth and efficiency.
            """)


class Opportunity_Calculation:
    def __init__(self):
        self.db_columns = None
        self.db_utils = None
        self.main_df = None

    def about(self):
        st.title('Welcome to Simfoni Opportunity Assessment Module')

        # Introduction
        st.write("""
            This module allows you to analyze and calculate various procurement opportunities to help streamline operations, 
            identify savings, and ensure better compliance with internal processes. Follow the steps below to get started.
            """)

        # Instructions
        st.subheader("How to Use the Opportunity Assessment Tool")

        st.write("""
            The Opportunity Assessment tool helps you evaluate multiple procurement aspects, such as supplier consolidation, 
            purchase price variance, payment terms, and more. To calculate and update these opportunities, follow the simple 
            steps below:
            """)

        # Step-by-step instructions
        st.markdown("""
            **Step 1: Fill Out the InScoping Form**  
            Before calculating opportunities, start by completing the InScoping form, which collects necessary details about 
            your client's procurement data. This form lays the foundation for the analysis.

            **Step 2: Calculate Opportunities**  
            Once the form is submitted, you can begin calculating each opportunity. To do this:
            1. Navigate to the opportunity you want to calculate (e.g., Supplier Consolidation, Purchase Price Variance).
            2. Click the **"Calculate"** button next to the specific opportunity.
            3. The tool will analyze the data and calculate the opportunity automatically.

            **Step 3: Review the Results**  
            After the calculation is complete, the results for that opportunity will be displayed. Review the details and assess 
            the findings to understand the potential cost savings or process improvements.

            **Step 4: Update the Database**  
            If you are satisfied with the calculated opportunity and its results:
            1. Click the **"Update"** button to store the results in the database.
            2. This step will flag the opportunity and save it for future reference or further analysis.

            **Step 5: Move on to the Next Opportunity**  
            Once you've updated the results in the database, repeat the same process for the other opportunities (such as contract compliance, payment terms, etc.).

            **Tip: Track Your Progress**  
            After each opportunity is updated, you can track your progress and ensure that all relevant flags have been set, giving you a clear view of the procurement landscape.
            """)

        # Conclusion
        st.write("""
            This tool is designed to make it easier for Simfoni clients to identify savings opportunities, improve compliance, and 
            standardize procurement practices. By following these steps, you'll be able to effectively calculate and track each 
            opportunity, ensuring better control and visibility over your procurement processes.
            """)

    def opportunity_initial_setup(self):
        st.write("Press the Initiate Config Button to Initiate the Configuration Process of Opportunity Assessment: ")
        if st.button('Initiate Config'):
            try:
                fetch_config_inputs = Fetch_configInputs()
                fetch_config_inputs.input(st.session_state.input_file_path)
                fetch_config_inputs.fill_details()
                Config.update_column_info()
                log_utils.setup_logger()
                st.toast('Config Setting Proccessed Successfully ', icon='🎉')
                time.sleep(2)
                st.toast('Please Navigate to Data Extraction Module of OA ', icon='🔜')
                self.db_utils = st.session_state['db_utils']
                self.db_columns = st.session_state['db_columns']
                if self.db_utils is None:
                    print('db_utils is None')
                else:
                    print('db_utils is not none')
            except Exception as e:
                st.toast("Unable To Proceed")
                st.error(f"{e} Error Occured")

    def db_table_extraction(self):
        st.write('Press the Extract Button to Begin the extraction of data: ')
        try:
            if st.button('Extract'):
                st.session_state['db_utils'].establish_connection()
                st.session_state['db_utils'].cursor = st.session_state['db_utils'].establish_cursor()
                st.toast('Table Extraction In Progress Please Wait')
                st.session_state['db_utils'].extracted_df = st.session_state['db_utils'].extract_table_db(
                    Config.dbColumnsForExtraction)
                self.main_df = st.session_state['db_utils'].extracted_df.copy()
                self.main_df = self.main_df.loc[:, ~self.main_df.columns.duplicated()]
                if self.main_df.shape[0] != 0:
                    st.toast('Table Extracted Successfully')
                    st.toast(f'{self.main_df.shape[0]} Rows Extracted')
                    invalids = ["#N/A", 'N/A', 'NA', 'NULL', 'NONE', 'NOT ASSIGNED', 'NOT AVAILABLE', " ", '0', ' ', '']
                    object_cols = self.main_df.select_dtypes(include='object')
                    ignore_cols = []
                    print(Config.column_info)
                    ignore_cols.append(Config.column_info['Serial No'])
                    ignore_cols.append(Config.column_info['Spend'])
                    for value in object_cols:
                        if value not in ignore_cols:
                            self.main_df[value] = self.main_df[value].apply(
                                lambda x: x.upper() if isinstance(x, str) else x
                            )
                            self.main_df[value] = self.main_df[value].apply(
                                lambda x: x.replace("  ", " ") if isinstance(x, str) else x
                            )
                            self.main_df[value] = self.main_df[value].replace(invalids, None)
                    st.toast('All Column Cleansed ', icon='🎊')
                    st.toast('Data Converted Successfully to Upper Case', icon='🎉')

        except Exception as e:
            print(e)
            log_utils.log_error(e)


if __name__ == "__main__":
    if 'current_page' not in st.session_state:
        st.session_state.current_page = 'Home_Page'
    if 'show_inscope_form' not in st.session_state:
        st.session_state.show_inscope_form = False
    if 'exclusion_form' not in st.session_state:
        st.session_state.exclusion_form = False

    st.sidebar.title('Navigation')
    page = st.sidebar.radio(
        'Automated OA',
        ['Home', 'InScoping App', 'Opportunity: About',
         'Opportunity: Config Setup',
         'Opportunity: Extract Table',
         'Opportunity: Supplier Consolidation',
         'Opportunity: Purchase Price Variance',
         'Opportunity: Contract Compliance']
    )

    st.session_state.current_page = page
    home = Home_Page()
    inscoping = InScoping_App()
    opportunity = Opportunity_Calculation()

    if st.session_state.current_page == 'Home' and not st.session_state.exclusion_form and not st.session_state.show_inscope_form:
        home.home_page()
    elif st.session_state.current_page == 'InScoping App' and not st.session_state[
        'show_inscope_form'] and not st.session_state.exclusion_form:
        inscoping.input()
    elif st.session_state.current_page == 'InScoping App' and st.session_state['show_inscope_form'] and not \
            st.session_state['exclusion_form']:
        inscoping.inscope_form()
    elif st.session_state.current_page == 'InScoping App' and st.session_state[
        'exclusion_form'] and not st.session_state.show_inscope_form:
        inscoping.exclusion_form()

    elif st.session_state.current_page == 'Opportunity: About':
        opportunity.about()

    elif st.session_state.current_page == 'Opportunity: Config Setup':
        opportunity.opportunity_initial_setup()

    elif st.session_state.current_page == 'Opportunity: Extract Table':
        opportunity.db_table_extraction()
