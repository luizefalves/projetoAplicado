import boto3
import time
import configparser


def main():

    config = configparser.ConfigParser()
    config.read('cred.conf')

    access_key = config['AWS']['KEY_PROCESSING']
    secret_access_key = config['AWS']['SECRET_PROCESSING']
    
    emr = boto3.client(
        'emr',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_access_key,
        region_name='us-east-2'
    )     
        
    start_resp = emr.start_notebook_execution(
        EditorId='e-6ENIR9ISXR7WC7J2KOTYZWJI6',
        RelativePath='noteAplicado.ipynb',
        ExecutionEngine={'Id':'j-9R5OIJA4GXK1'},
        ServiceRole='EMR_Notebooks_DefaultRole'
    )

    execution_id = start_resp["NotebookExecutionId"]
    print(execution_id)
    print("\n")
        
    describe_response = emr.describe_notebook_execution(NotebookExecutionId=execution_id)
        
    print(describe_response)
    print("\n")
        
    list_response = emr.list_notebook_executions()
    print("Existing notebook executions:\n")
    for execution in list_response['NotebookExecutions']:
        print(execution)
        print("\n")  
        
    print("Sleeping for 5 sec...")
    time.sleep(120)
        
    print("Stop execution " + execution_id)
    emr.stop_notebook_execution(NotebookExecutionId=execution_id)
    describe_response = emr.describe_notebook_execution(NotebookExecutionId=execution_id)
    print(describe_response)
    print("\n")    

    return print("Data was processed")

if __name__ == "__main__":
    main()



    



