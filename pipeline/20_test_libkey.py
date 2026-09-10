"""Minimal test: exact same setup that showed LibKey before."""
import sys, os, time, shutil
os.environ["PYTHONIOENCODING"] = "utf-8"
from pathlib import Path

CHROME_USER_DATA = Path(r"C:\Users\brouw\AppData\Local\Google\Chrome\User Data")
TEMP_PROFILE = Path(r"C:\Users\brouw\Documents\PhD\Projects\LiteratureQueeryThalamusTime\Data\interim\chrome_profile_copy")

print("Step 1: Copy profile...")
sys.stdout.flush()
if TEMP_PROFILE.exists():
    shutil.rmtree(TEMP_PROFILE, ignore_errors=True)
    time.sleep(1)
TEMP_PROFILE.mkdir(parents=True)
temp_default = TEMP_PROFILE / "Default"
temp_default.mkdir()
src = CHROME_USER_DATA / "Default"
for name in ["Cookies", "Cookies-journal", "Login Data", "Login Data-journal",
             "Web Data", "Web Data-journal", "Preferences", "Secure Preferences",
             "Network"]:
    s = src / name
    d = temp_default / name
    if s.exists():
        if s.is_dir():
            shutil.copytree(str(s), str(d))
        else:
            shutil.copy2(str(s), str(d))
        print(f"  copied {name}")
ls = CHROME_USER_DATA / "Local State"
if ls.exists():
    shutil.copy2(str(ls), str(TEMP_PROFILE / "Local State"))
    print("  copied Local State")
sys.stdout.flush()

print("\nStep 2: Launch Chrome...")
sys.stdout.flush()

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

options = Options()
options.add_argument(f"--user-data-dir={TEMP_PROFILE}")
options.add_argument("--profile-directory=Default")
options.add_argument("--no-first-run")
options.add_argument("--no-default-browser-check")
options.add_argument("--disable-extensions")
options.page_load_strategy = "eager"

service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=options)
driver.set_page_load_timeout(15)
print("  OK!")
sys.stdout.flush()

print("\nStep 3: Go to google.com (sanity check)...")
sys.stdout.flush()
try:
    driver.get("https://www.google.com")
    time.sleep(2)
    print(f"  URL: {driver.current_url}")
    print(f"  Title: {driver.title}")
except Exception as e:
    print(f"  FAILED: {e}")
sys.stdout.flush()

print("\nStep 4: Go to LibKey...")
sys.stdout.flush()
try:
    driver.get("https://libkey.io/libraries/320/10.1016/j.neuron.2017.09.017")
    time.sleep(5)
    print(f"  URL: {driver.current_url}")
    print(f"  Title: {driver.title}")
    body = driver.find_element("tag name", "body").text[:300]
    print(f"  Body: {body}")
except Exception as e:
    print(f"  FAILED: {e}")
sys.stdout.flush()

print("\nKeeping open 20s...")
try:
    time.sleep(20)
except KeyboardInterrupt:
    pass

driver.quit()
print("Done.")
