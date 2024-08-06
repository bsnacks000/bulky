import requests
import time
import sys


def main():
    resp = requests.post("http://localhost:8080/file")
    resp.raise_for_status()

    response = resp.json()
    task_id = response["task_id"]
    task_status = response["task_status"]
    poll_time = 0.1
    while True:
        time.sleep(poll_time)
        resp = requests.get(f"http://localhost:8080/file/task/{task_id}")
        resp.raise_for_status()

        response = resp.json()
        task_id = response["task_id"]
        task_status = response["task_status"]
        if task_status == "SUCCESS":
            print(task_status, file=sys.stderr)
            break
        elif task_status == "FAILED":
            print(task_status, file=sys.stderr)
            raise RuntimeError(f"{task_id} failed.")
        else:
            print(task_status, file=sys.stderr)

        poll_time += 0.1
        if poll_time > 1:
            raise TimeoutError("poll timeout")

    # should be able to fetch the zip file now
    response = requests.get(f"http://localhost:8080/file/{task_id}")
    response.raise_for_status()

    with open("myfile.zip", "wb") as f:
        for chunk in response.iter_content(chunk_size=512):
            if chunk:  # filter out keep-alive new chunks
                f.write(chunk)

    print(resp.content)


if __name__ == "__main__":
    main()
