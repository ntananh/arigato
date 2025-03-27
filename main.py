from plugins.job_board_hooks.linkedin_hook import LinkedinHook
import pandas as pd

if __name__ == "__main__":
    linkedin_hook = LinkedinHook()
    jobs = linkedin_hook.run()
    df = pd.DataFrame(jobs)
    df.to_csv('data/linkedin_jobs.csv', index=False, encoding='utf-8')

    print(df)