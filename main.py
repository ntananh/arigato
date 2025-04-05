from plugins.job_board_hooks.rapid_api_hook import DynamicRapidApiHook
import logging

logging.basicConfig(level=logging.INFO)

if __name__ == "__main__":
    try:
        hook = DynamicRapidApiHook.create_hook(
            api_name='linkedin',
            api_key="5c83b411d0mshf1969ca27915edbp140b7bjsn24c57888efba"
        )
        jobs = hook.search_jobs(
            job_type='Software Engineer',
            location='United States',
            keywords=['Python', 'Backend']
        )
        print(f"Jobs retrieved: {len(jobs)}")
    except Exception as e:
        logging.error(f"An error occurred: {e}")