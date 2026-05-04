import time
import os
import pandas as pd

from selenium import webdriver
from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


class SeleniumWebDriverContextManager:
    def __init__(self):
        self.driver: WebDriver | None = None

    def __enter__(self):
        options = Options()
        options.add_argument("--start-maximized")
        # options.add_argument("--headless=new")

        self.driver = webdriver.Chrome(options=options)
        return self.driver

    def __exit__(self, exc_type, exc_value, traceback):
        if self.driver:
            self.driver.quit()


def save_table(driver: WebDriver):
    wait = WebDriverWait(driver, 15)

    table = wait.until(
        EC.visibility_of_element_located((By.CLASS_NAME, "table"))
    )

    columns = table.find_elements(By.CLASS_NAME, "y-column")

    data = {}

    for column in columns:
        header = column.find_element(By.ID, "header").text.strip()

        cells = column.find_elements(By.CLASS_NAME, "cell-text")
        values = [
            cell.text.strip()
            for cell in cells
            if cell.text.strip() and cell.text.strip() != header
        ]

        data[header] = values

    df = pd.DataFrame(data)
    df.to_csv("table.csv", index=False)

    print("table.csv saved")


def extract_doughnut_data(driver: WebDriver):
    wait = WebDriverWait(driver, 15)

    pie = wait.until(
        EC.presence_of_element_located((By.CLASS_NAME, "pielayer"))
    )

    labels = pie.find_elements(
        By.CSS_SELECTOR,
        "text.slicetext[data-notex='1']"
    )

    rows = []

    for label in labels:
        tspans = label.find_elements(By.TAG_NAME, "tspan")

        if len(tspans) >= 2:
            rows.append({
                "Facility Type": tspans[0].text.strip(),
                "Min Average Time Spent": tspans[1].text.strip()
            })

    return pd.DataFrame(rows)


def save_doughnut_state(driver: WebDriver, index: int):
    wait = WebDriverWait(driver, 15)

    graph = wait.until(
        EC.presence_of_element_located((By.CLASS_NAME, "plotly-graph-div"))
    )

    graph.screenshot(f"screenshot{index}.png")

    df = extract_doughnut_data(driver)
    df.to_csv(f"doughnut{index}.csv", index=False)

    print(f"screenshot{index}.png saved")
    print(f"doughnut{index}.csv saved")


def process_doughnut(driver: WebDriver):
    wait = WebDriverWait(driver, 15)

    save_doughnut_state(driver, 0)

    scrollbox = wait.until(
        EC.presence_of_element_located((By.CLASS_NAME, "scrollbox"))
    )

    legend_items = scrollbox.find_elements(By.CLASS_NAME, "traces")

    for index in range(len(legend_items)):
        try:
            scrollbox = driver.find_element(By.CLASS_NAME, "scrollbox")
            legend_items = scrollbox.find_elements(By.CLASS_NAME, "traces")
            item = legend_items[index]

            driver.execute_script(
                "arguments[0].scrollIntoView({block: 'center'});",
                item
            )

            time.sleep(0.5)
            item.click()
            time.sleep(1)

            save_doughnut_state(driver, index + 1)

        except Exception as error:
            print(f"Failed to process doughnut filter {index + 1}: {error}")


if __name__ == "__main__":
    with SeleniumWebDriverContextManager() as driver:
        file_path = os.path.abspath("../Robot Framework/data/report.html")
        driver.get(f"file:///{file_path.replace(os.sep, '/')}")

        save_table(driver)
        process_doughnut(driver)