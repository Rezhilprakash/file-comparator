import time

import requests
import streamlit as st
import pandas as pd
import numpy as np
import io
import zipfile
from streamlit_lottie import st_lottie
import os

def finding_diff_col(df1,df2):
    try:
        print('Finding Diff columns')
        df1_cols = df1.columns
        df2_cols = df2.columns

        ## Finding the difference columns
        diff_cols = df1_cols.difference(df2_cols)

        ## Removing the difference columns
        df1.drop(columns=diff_cols,inplace=True)
        return df1,diff_cols
    except Exception as e:
        print('Error finding column:'+str(e))
        
def finding_common_col(df1,df2):
    try:
        print('Finding common columns')
        df1_cols = df1.columns.str.lower()
        df2_cols = df2.columns.str.lower()

        ## Finding the common columns and difference columns
        common_cols = list(set(df1_cols).intersection(df2_cols))
        print('Common columns',common_cols)
        return common_cols
    except Exception as e:
        print('Error finding column:'+str(e))
        
def finding_extra_rows(df1,df2,key):
    try:
        print('Finding extra rows')
        merged_df = df1.merge(df2, on=key, how='left', indicator=True)
        missing_rows = merged_df[merged_df['_merge'] == 'left_only']
        missing_rows = missing_rows.drop(columns='_merge')
        values_to_remove = missing_rows[key].tolist()
        filtered_df = ~df1[key].isin(values_to_remove)
        print(filtered_df)
        df1 = df1[filtered_df]
        print(df1)
        return df1,values_to_remove
    except Exception as e:
        print('Error finding extra rows:'+str(e))
        
# Function to apply color to updated cells
def highlight_updated_cells(value):
    try:
        if '-->' in str(value):
            return 'color: red'
        return ''
    except Exception as e:
        print('Error highlighting cells:'+str(e))

def find_duplicates(df,key):
    try:
        print('Finding duplicates')
        duplicates = df[df.duplicated(subset=[key], keep=False)]
        values_to_remove = duplicates[key].tolist()
        filtered_df = ~df[key].isin(values_to_remove)
        df = df[filtered_df]
        return df,values_to_remove
    except Exception as e:
        print('Error finding duplicates:'+str(e))
        
def file_comparison(source,dest,key):
    status_placeholder.warning('Finding duplicate and extra rows...',icon="⏳")
    print('Comparing files...')
    processed_rows = 0
    total_rows = len(source)
    print(processed_rows)
    print(total_rows)
    source,diff_cols_source=finding_diff_col(source,dest)
    dest,diff_cols_dest=finding_diff_col(dest,source)     
    
    source_filtered,values_to_remove_source=finding_extra_rows(source,dest,key)
    dest_filtered,values_to_remove_dest=finding_extra_rows(dest,source,key) 
    
#      Find duplicate rows based on key
    source_filtered_rows,duplicates_source=find_duplicates(source_filtered,key)
    dest_filtered_rows,duplicates_dest=find_duplicates(dest_filtered,key) 
    
    status_placeholder.warning('Comparing values...',icon="⏳")
    
    comparison_values = source_filtered_rows.values == dest_filtered_rows.values
    print(comparison_values)
    # Updating the values with oldvalues-->newvalues format
    rows,cols=np.where(comparison_values==False)
    print('Rows',rows)
    print('Cols',cols)
    progress_bar = st.progress(0, text='Started comparing files...')
    
    for item in zip(rows,cols):
        # print('Entered for loop')
        print(f"Row: {item[0]}, Column: {item[1]}")
        # print("DataFrame Indexes:", source_filtered_rows.index)
        # print("DataFrame Columns:", source_filtered_rows.columns)
        # print(source_filtered_rows.iloc[item[0], item[1]])
        # print(dest_filtered_rows.iloc[item[0], item[1]])
        
        processed_rows = item[0] + 1
        
        # Update progress bar
        progress = int((processed_rows / total_rows) * 100)
        progress_text = f"Started comparing files. Processing... {str(processed_rows)}/{str(total_rows)} rows"
        # print(progress_text)
        
        progress_bar.progress(progress, text=progress_text)
        source_filtered.iloc[item[0], item[1]] = '{} --> {}'.format(source_filtered.iloc[item[0], item[1]],dest_filtered.iloc[item[0], item[1]])
        
        
    print(source_filtered)
    ## Replacing the nan --> nan values to ''
    source_filtered = source_filtered.replace(['nan --> nan'], '')
    source_filtered = source_filtered.replace(['NaT --> NaT'], '')
    status_placeholder.empty()
    return diff_cols_source,diff_cols_dest,values_to_remove_source,values_to_remove_dest,duplicates_source,duplicates_dest,source_filtered

def read_file_with_encoding(file_path):
    """
    Reads both Excel and CSV files, handling encoding issues for CSV files.

    Args:
        file_path (str): Path to the file (Excel or CSV).

    Returns:
        pd.DataFrame: DataFrame containing the contents of the file.
    """
    # Try to read Excel files
    if file_path.name.endswith(('.xls', '.xlsx')):
        try:
            cols = pd.read_excel(file_path,nrows=0).columns.tolist()
            print('Cols',cols)
            df = pd.read_excel(file_path)
            return df
        except Exception as e:
            raise Exception(f"Error reading Excel file: {e}")
    
    # Try to read CSV files, with handling of different encodings
    elif file_path.name.endswith('.csv'):
        encodings = ['utf-8', 'latin1', 'ISO-8859-1', 'utf-16','cp1252']
        
        for encoding in encodings:
            try:
                # cols = pd.read_csv(file_path,nrows=0).columns.tolist()
                # print('Cols',cols)
                df = pd.read_csv(file_path,encoding='ISO-8859-1')
                return df
            except UnicodeDecodeError:
                continue  # Try the next encoding
        
        raise Exception("Unable to read CSV file with common encodings")
    
    else:
        raise Exception("Unsupported file format. Please upload a CSV or Excel file.")

def get_cols(file_path):
    """
    Reads both Excel and CSV files, handling encoding issues for CSV files.

    Args:
        file_path (str): Path to the file (Excel or CSV).

    Returns:
        pd.DataFrame: DataFrame containing the contents of the file.
    """
    # Try to read Excel files
    if file_path.name.endswith(('.xls', '.xlsx')):
        try:
            cols = pd.read_excel(file_path,nrows=0).columns.tolist()
            print('Cols',cols)
            return cols
        except Exception as e:
            raise Exception(f"Error reading Excel file: {e}")
    
    # Try to read CSV files, with handling of different encodings
    elif file_path.name.endswith('.csv'):
        encodings = ['utf-8', 'latin1', 'ISO-8859-1', 'utf-16','cp1252']
        
        for encoding in encodings:
            try:
                cols = pd.read_csv(file_path,nrows=0,encoding='ISO-8859-1').columns.tolist()
                return cols
            except UnicodeDecodeError:
                continue  # Try the next encoding
        
        raise Exception("Unable to read CSV file with common encodings")
    
    else:
        raise Exception("Unsupported file format. Please upload a CSV or Excel file.")

def file_comparison_main(source,dest,source_key,dest_key):
    try:        
        print('File started comparing - Main')
        status_placeholder.warning('Started processing files...Sorting and Renaming columns',icon="⏳")
        
        source = source.applymap(lambda x: x.replace('_x000D_', '') if isinstance(x, str) else x)
        source = source.applymap(lambda x: x.replace('\n', ' ') if isinstance(x, str) else x)
        dest = dest.applymap(lambda x: x.replace('_x000D_', '') if isinstance(x, str) else x)
        dest = dest.applymap(lambda x: x.replace('\n', ' ') if isinstance(x, str) else x)
        print('Dest cols',dest.columns)
        #Renaming key in destination column
        key = source_key
        print(source_key,'Source key')
        print(dest_key)
        if dest_key in dest:
            print('True')
            dest.rename(columns={dest_key: key}, inplace=True)

        #Moving key columns as first
        source = source[[key] + [col for col in source.columns if col != key]]
        dest = dest[[key] + [col for col in dest.columns if col != key]]
        
        # Sort columns in alphabetical order except for the first column
        sorted_remaining_cols = sorted(source.columns[1:])
        sorted_columns = [source.columns[0]] + sorted_remaining_cols
        source = source[sorted_columns]
        sorted_remaining_cols1 = sorted(dest.columns[1:])
        sorted_columns1 = [dest.columns[0]] + sorted_remaining_cols1
        dest = dest[sorted_columns1]
        ## sorting values based on key column
        print(source.columns)
        print(dest.columns)
        source.sort_values(source_key,inplace=True)
        dest.sort_values(key,inplace=True)
        print('Sorted columns')
        
        diff_cols_source,diff_cols_dest,extra_rows_source,extra_rows_dest,duplicates_source,duplicates_dest,df=file_comparison(source,dest,key)
        df.rename(columns={key: dest_key}, inplace=True)  
        # Apply the style to the dataframe
        styled_df = df.style.applymap(highlight_updated_cells)
        
        print(styled_df)  
        return diff_cols_source,diff_cols_dest,extra_rows_source,extra_rows_dest,duplicates_source,duplicates_dest,styled_df
   
    except Exception as e: 
        print('Error comparing files:'+str(e))
        st.session_state.error = str(e)  
    
def create_zip_and_download(source_file_name, diff_cols_source,diff_cols_dest,extra_rows_source,extra_rows_dest,duplicates_source, duplicates_dest,styled_df):
    # Save styled DataFrame to an Excel file in memory
    excel_buffer = io.BytesIO()
    with pd.ExcelWriter(excel_buffer, engine="openpyxl") as writer:
        # Use the Styler's `to_excel` method
        styled_df.to_excel(writer, sheet_name="source_file_name", index=False)
    excel_buffer.seek(0)

    # Save duplicates to a text file in memory
    txt_buffer = io.StringIO()  # Use StringIO instead of BytesIO for text
    text_file = txt_buffer
    text_file.write("Columns in source but not in destination - ")
    text_file.write(str(diff_cols_source.tolist()))
    text_file.write("\n\n")
    text_file.write("Columns in destination but not in source - ")
    text_file.write(str(diff_cols_dest.tolist()))
    text_file.write("\n\n")
    text_file.write("Extra rows in source - Count ->")
    text_file.write(str(len(extra_rows_source)))
    text_file.write(str(extra_rows_source))
    text_file.write("\n\n")
    text_file.write("Extra rows in destination - Count ->")
    text_file.write(str(len(extra_rows_dest)))
    text_file.write(str(extra_rows_dest))
    text_file.write("\n\n")
    text_file.write("Duplicate rows in source - ")
    text_file.write(str(duplicates_source))
    text_file.write("\n\n")
    text_file.write("Duplicate rows in destination - ")
    text_file.write(str(duplicates_dest))
    txt_buffer.seek(0)

    # Create a zip file in memory
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w") as zip_file:
        zip_file.writestr(f"{source_file_name}_comparison_results.xlsx", excel_buffer.getvalue())
        zip_file.writestr(f"{source_file_name}_result.txt", txt_buffer.getvalue())
    zip_buffer.seek(0)

    return zip_buffer

def preserve_styler_for_first_rows(styled_df, num_rows=10):
    # Extract the original DataFrame from the Styler object
    original_df = styled_df.data
    
    # Select first few rows
    first_rows = original_df.head(num_rows)
    
    # Recreate the Styler for the first rows
    # This preserves any custom styling from the original Styler
    styled_first_rows = first_rows.style
    
    # Copy over any styling from the original Styler
    styled_first_rows._todo = styled_df._todo.copy()
    
    return styled_first_rows

def render_animation():
        animation_response = requests.get('https://lottie.host/f320542b-5764-4dc8-9039-2b801fba80ae/EfBjUsmqmW.json')
        animation_json = dict()
        
        if animation_response.status_code == 200:
            animation_json = animation_response.json()
        else:
            print("Error in the URL")     
                            
        return st_lottie(animation_json, height=300, width=400)

path=os.getcwd( )
print(path)

def writing_report(diff_cols_stage,diff_cols_qa,values_to_remove_stage,values_to_remove_qa,duplicates_stage,duplicates_qa,name):
        try:
            print('Writing report')
            #creating a new dir


            path1 = path+'\\'+name

            if not os.path.exists(path1):
                os.mkdir(path1)

            print(path1)
            # write data in a file.
            file1 = open(path1+'//'+name+".txt","w")

            # \n is placed to indicate EOL (End of Line)
            file1.write("Columns in source but not in destination - ")
            file1.write(str(diff_cols_stage.tolist()))
            file1.write("\n\n")
            file1.write("Columns in destination but not in source - ")
            file1.write(str(diff_cols_qa.tolist()))
            file1.write("\n\n")
            file1.write("Extra rows in source - Count ->")
            file1.write(str(len(values_to_remove_stage)))
            file1.write(str(values_to_remove_stage))
            file1.write("\n\n")
            file1.write("Extra rows in destination - Count ->")
            file1.write(str(len(values_to_remove_qa)))
            file1.write(str(values_to_remove_qa))
            file1.write("\n\n")
            file1.write("Duplicate rows in source - ")
            file1.write(str(duplicates_stage))
            file1.write("\n\n")
            file1.write("Duplicate rows in destination - ")
            file1.write(str(duplicates_qa))
            file1.close()
            print('Done writing report')
        except Exception as e:
            print('Error writing report:',str(e))


st.set_page_config(layout="wide")

  
st.markdown("""
<style>
:root {
  --primary-color: #d45608;
  --background-color: #0e1117;
  --secondary-background-color: #262730;
  --text-color: #fafafa;
}
</style>
""", unsafe_allow_html=True)


# Multiple style variations
st.markdown("""
    <style>
    /* Flat design */
    .stButton > button {
         background-color: #d45608;
        color: white;
        border-radius: 8px !important;
        border: none;
        padding: 5px 20px;
         box-shadow: 0 4px 6px rgba(0,0,0,0.1);
         justify-content:end
    }
    

    .stButton > button:hover {
        box-shadow: 0 4px 6px #252f458a;
        color:white;
        border:none !important
    }
    
    .stButton > button:focus {
        color:white !important;
         background-color: #d45608 !important;
    }
    
    .stButton > button:active {
        box-shadow: 0 4px 4px #252f458a;
        color:#fc7643 !important;
         background-color: #252f45 !important;
    }
    
    .stDownloadButton > button {
        display: inline-flex;
        align-items: center;
        justify-content: center;
        font-weight: bold;
        border: 2px solid #FC7643 !important;
        color:#FC7643
    }
    
    .stDownloadButton > button:hover {
        box-shadow: 0 4px 6px #252f458a;
        background-color:#FC7643 !important;
        color:white !important;
    }
    
    .centered-title {
        text-align: center;
        padding-bottom: 10px;
        font-size: 2em;
        font-weight: bold;
    }
    .centered-tag {
        text-align: center;
        padding-bottom: 15px;
        font-size: 1em;
    }
    </style>
""", unsafe_allow_html=True)

# Streamlit UI
st.markdown("<h3 class='centered-title'>🗂️ File Comparator</h3>", unsafe_allow_html=True)
st.markdown("<h6 class='centered-tag'>Upload two files to get the results.</h6>", unsafe_allow_html=True)


if "compare_clicked" not in st.session_state:
            st.session_state.compare_clicked = False
if "selected_column" not in st.session_state:
            st.session_state.selected_column = None
if "selected_column1" not in st.session_state:
            st.session_state.selected_column1 = None
if "processed_rows" not in st.session_state:
            st.session_state.processed_rows = 0
if "comparison_completed" not in st.session_state:
            st.session_state.comparison_completed = False
if "comparison_results" not in st.session_state:
    st.session_state.comparison_results = {
        'diff_cols_source': None,
        'diff_cols_dest': None,
        'extra_rows_source': None,
        'extra_rows_dest': None,
        'duplicates_source': None,
        'duplicates_dest': None,
        'styled_df': None
    }
if "error" not in st.session_state:
    st.session_state.error = ""
if "uploader_key" not in st.session_state:
    st.session_state.uploader_key=0
if "uploader_key1" not in st.session_state:
    st.session_state.uploader_key1=0
if "columns_fetched" not in st.session_state:
    st.session_state.columns_fetched = False
if "col1" not in st.session_state:
    st.session_state.col1 = None
if "col2" not in st.session_state:
    st.session_state.col2 = None
    
status_placeholder = None

if not st.session_state.comparison_completed and not st.session_state.compare_clicked:
    col1, col2,col3 = st.columns([1, 2,1])  # Equal width columns
    with col2:
            upload_container1 = st.container(border = True)
            # File uploaders in the left column
            upload_container1.markdown(
                    f"<div style='font-size:20px;padding-block:5px;font-weight:bold'>Upload Files</div>",
                    unsafe_allow_html=True)
            file1 = upload_container1.file_uploader("Upload the source file", type=['csv', 'xls', 'xlsx'],key=f"uploader_{st.session_state.uploader_key}")
            file2 = upload_container1.file_uploader("Upload the destination file", type=['csv', 'xls', 'xlsx'],key=f"uploader1_{st.session_state.uploader_key1}")
            if file1 and file2:
                if status_placeholder is None:
                    status_placeholder = st.empty()
                    
                if not st.session_state.columns_fetched:
                    status_placeholder.warning("Fetching columns from source and destination files", icon="⏳")
                    col1 = get_cols(file1)
                    col2 = get_cols(file2)
                    status_placeholder.empty()

                    if col1 is not None and col2 is not None:
                        st.session_state.columns_fetched = True
                        st.session_state.col1 = col1
                        st.session_state.col2 = col2
                    else:
                        st.error("Failed to fetch columns from files.")
                        st.stop()
                else:
                    col1 = st.session_state.col1
                    col2 = st.session_state.col2
                    
                if col1 is not None and col2 is not None:
                                        
                    radio1,radio2 = upload_container1.columns([2,2])
                    # Display  columns as radio buttons
                    col1_selected = radio1.radio("Select Id column from source file:", col1,index=None,horizontal=False, key="col1_radio")                                                                                                                
                    col2_selected = radio2.radio("Select Id column from destination file:", col2,index=None, horizontal=False, key="col2_radio")

                    # Update the session state when a column is selected
                    if col1_selected:
                        st.session_state.selected_column = col1_selected
                    if col2_selected:
                        st.session_state.selected_column1 = col2_selected
                        
                    if st.session_state.selected_column and st.session_state.selected_column1:
                        if not st.session_state.compare_clicked:
                            status_placeholder.empty()
                            upload_container1.info(f"Selected column Source: {st.session_state.selected_column} | Destination: {st.session_state.selected_column1} ", icon="ℹ️")
                            # Create a horizontal layout for buttons
                            button_col1, button_col2, button_col3, button_col4 = upload_container1.columns([2.5, 3,3,1.5])
                                                                  
                            # Compare button
                            compare_button = button_col4.button("Compare", key="compare_main_button",disabled = st.session_state.compare_clicked )
                            
                        if compare_button:
                            st.session_state.compare_clicked = True
                            st.session_state.comparison_completed = False
                            st.session_state.error = ""
                            st.session_state.processed_rows = 0
                            # Add a loader with progress
                            status_placeholder = st.warning('Reading files...',icon="⏳")
                            
                            df1 = read_file_with_encoding(file1)
                            df2 = read_file_with_encoding(file2)
                            print(df2.head(5))
                            # Calling the comparison function
                            if file1 and file2 and st.session_state.compare_clicked:
                                try:
                                    st.session_state.comparison_results['diff_cols_source'], \
                                    st.session_state.comparison_results['diff_cols_dest'], \
                                    st.session_state.comparison_results['extra_rows_source'], \
                                    st.session_state.comparison_results['extra_rows_dest'], \
                                    st.session_state.comparison_results['duplicates_source'], \
                                    st.session_state.comparison_results['duplicates_dest'], \
                                    st.session_state.comparison_results['styled_df'] = file_comparison_main(df1, df2,col1_selected,col2_selected)
                                    # Simulate some loading time
                                    # time.sleep(10)
                                    print(st.session_state.comparison_results['duplicates_dest'])
                                    print('styled_df',st.session_state.comparison_results['styled_df'])
                                    st.session_state.comparison_completed = True
                                    st.session_state.compare_clicked = False
                                    st.rerun()           
                                except Exception as e:
                                    print('Error',str(e))
                                    st.session_state.comparison_completed = True
                                    st.session_state.compare_clicked = False
                                    st.rerun()    
                                                                               
else:    
    # Create two columns with specific width ratios
    col1, col2,col3,col4,col5 = st.columns([1,3,0.25,2.5, 1])  # Equal width columns
    print(st.session_state.error)
    with col2:
            upload_container1 = st.container(border = True)
            # File uploaders in the left column
            upload_container1.markdown(
                    f"<div style='font-size:20px;padding-block:5px;font-weight:bold'>Upload Files</div>",
                    unsafe_allow_html=True)
            file1 = upload_container1.file_uploader("Upload the source file", type=['csv', 'xls', 'xlsx'],key=f"uploader_{st.session_state.uploader_key}")
            file2 = upload_container1.file_uploader("Upload the destination file", type=['csv', 'xls', 'xlsx'],key=f"uploader1_{st.session_state.uploader_key1}")
            if file1 and file2:
                if status_placeholder is None:
                    status_placeholder = st.empty()
                    
                if not st.session_state.columns_fetched:
                    status_placeholder.warning("Fetching columns from source and destination files", icon="⏳")
                    col1 = get_cols(file1)
                    col2 = get_cols(file2)
                    status_placeholder.empty()

                    if col1 is not None and col2 is not None:
                        st.session_state.columns_fetched = True
                        st.session_state.col1 = col1
                        st.session_state.col2 = col2
                    else:
                        st.error("Failed to fetch columns from files.")
                        st.stop()
                else:
                    col1 = st.session_state.col1
                    col2 = st.session_state.col2
                    
                if col1 is not None and col2 is not None:
                                        
                    radio1,radio2 = upload_container1.columns([2,2])
                    # Display  columns as radio buttons
                    col1_selected = radio1.radio("Select Id column from source file:", col1,index=None,horizontal=False, key="col1_radio")                                                                                                                
                    col2_selected = radio2.radio("Select Id column from destination file:", col2,index=None, horizontal=False, key="col2_radio")

                    # Update the session state when a column is selected
                    if col1_selected:
                        st.session_state.selected_column = col1_selected
                    if col2_selected:
                        st.session_state.selected_column1 = col2_selected
                        
                    if st.session_state.selected_column and st.session_state.selected_column1:
                        if not st.session_state.compare_clicked:
                            status_placeholder.empty()
                            upload_container1.info(f"Selected column Source: {st.session_state.selected_column} | Destination: {st.session_state.selected_column1} ", icon="ℹ️")
                            # Create a horizontal layout for buttons
                            button_colr1, button_colr2, button_colr3, button_colr4 = upload_container1.columns([3.5, 3.5,1.5,2])
                                                       
                            # Modified compare button logic
                            compare_button = button_colr4.button("Compare", key="compare_button",disabled = st.session_state.compare_clicked)
                            reset_button = button_colr3.button("Reset", key="reset_button",type="secondary",help="Reset the comparison",)
                        if compare_button:
                            st.session_state.compare_clicked = True
                            st.session_state.comparison_completed = False
                            st.session_state.error = ""
                            st.session_state.processed_rows = 0
                            # Add a loader with progress
                            status_placeholder = st.warning('Reading files...',icon="⏳")
                            
                            df1 = read_file_with_encoding(file1)
                            df2 = read_file_with_encoding(file2)
                            # Calling the comparison function
                            if file1 and file2 and st.session_state.compare_clicked:
                                try:
                                    st.session_state.comparison_results['diff_cols_source'], \
                                    st.session_state.comparison_results['diff_cols_dest'], \
                                    st.session_state.comparison_results['extra_rows_source'], \
                                    st.session_state.comparison_results['extra_rows_dest'], \
                                    st.session_state.comparison_results['duplicates_source'], \
                                    st.session_state.comparison_results['duplicates_dest'], \
                                    st.session_state.comparison_results['styled_df'] = file_comparison_main(df1, df2,col1_selected,col2_selected)
                                    # Simulate some loading time
                                    # time.sleep(10)
                                    print(st.session_state.comparison_results['duplicates_dest'])
                                    print('styled_df',st.session_state.comparison_results['styled_df'])
                                    st.session_state.comparison_completed = True
                                    st.session_state.compare_clicked = False
                                    st.rerun()           
                                except Exception as e:
                                    print('Error',str(e))
                                    st.session_state.comparison_completed = True
                                    st.session_state.compare_clicked = False
                                    st.rerun()    
                        if reset_button:
                            # Reset only the specific states related to comparison
                            reset_states = {
                                'compare_clicked': False,
                                'selected_column': None,
                                'selected_column1' : None,                                
                                'processed_rows': 0,
                                'comparison_completed': False,
                                'comparison_results': {
                                    'diff_cols_source': None,
                                    'diff_cols_dest': None,
                                    'extra_rows_source': None,
                                    'extra_rows_dest': None,
                                    'duplicates_source': None,
                                    'duplicates_dest': None,
                                    'styled_df': None,
                                    
                                },
                                'error': "",
                                'file1': None,
                                'file2': None
                            }
                            
                            # Update states in a single operation
                            for key, value in reset_states.items():
                                st.session_state[key] = value
                            
                            # Increment keys only if they exist
                            st.session_state.uploader_key = st.session_state.get('uploader_key', 0) + 1
                            st.session_state.uploader_key1 = st.session_state.get('uploader_key1', 0) + 1
                            
                            st.rerun()
    with col4:
        if file1 and file2 and st.session_state.compare_clicked:
            print('Loading animation')
            render_animation()
        # Comparison results in the right column
        elif file1 and file2 and st.session_state.comparison_completed and st.session_state.error == '':
                st.markdown(
                f"<div style='font-size:20px;padding-block:3px;font-weight:bold'>Status</div>",
                unsafe_allow_html=True)
                st.success(f"Comparison Successful",icon="✅")
                # Display styled DataFrame preview
                st.markdown(
                f"<div style='font-size:20px;padding-block:3px;font-weight:bold'>Comparison Results</div>",
                unsafe_allow_html=True)
                styled_first_rows = preserve_styler_for_first_rows(st.session_state.comparison_results['styled_df'],15)

                st.dataframe(styled_first_rows, use_container_width=True)
                
                source_file_name = file1.name.split('.')[0]  # Use source file name

                writing_report(st.session_state.comparison_results['diff_cols_source'],
                st.session_state.comparison_results['diff_cols_dest'], st.session_state.comparison_results['extra_rows_source'],
                st.session_state.comparison_results['extra_rows_dest'],st.session_state.comparison_results['duplicates_source'],
                st.session_state.comparison_results['duplicates_dest'],source_file_name)
                
                st.session_state.comparison_results['styled_df'].to_excel(path+'\\'+source_file_name+'\\'+source_file_name+".xlsx", sheet_name=source_file_name, index=False)

                zip_buffer = create_zip_and_download(
                        source_file_name, 
                    st.session_state.comparison_results['diff_cols_source'], 
                        st.session_state.comparison_results['diff_cols_dest'], 
                        st.session_state.comparison_results['extra_rows_source'], 
                        st.session_state.comparison_results['extra_rows_dest'], 
                        st.session_state.comparison_results['duplicates_source'], 
                        st.session_state.comparison_results['duplicates_dest'],
                        st.session_state.comparison_results['styled_df']
                    )
                
                # Directly trigger download 
                st.download_button(
                        label="Download Results as ZIP",
                        data=zip_buffer,
                        file_name=f"{source_file_name}_results.zip",
                        mime="application/zip"
                    )
        elif  st.session_state.error != '':
            print('Error component')
            st.markdown(
                f"<div style='font-size:20px;padding-block:3px;font-weight:bold'>Status</div>",
                unsafe_allow_html=True)
            st.error(st.session_state.error,icon="⛔")