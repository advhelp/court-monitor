"""
Test the ЄДРСР HTML parser with sample data.
Run locally: python test_parser.py
"""
from edrsr_monitor import parse_edrsr_html, decision_uid

# Sample HTML that mimics ЄДРСР search results structure
SAMPLE_HTML = """
<html>
<body>
<table>
<tr>
  <td class="RegNumber"><a href="/Review/12345678">12345678</a></td>
  <td class="VRType">Ухвала</td>
  <td class="RegDate">10.04.2026</td>
  <td class="LawDate">08.04.2026</td>
  <td class="CSType">Цивільне</td>
  <td class="CaseNumber">199/2348/23</td>
  <td class="CourtName">Індустріальний районний суд м.Дніпропетровська</td>
  <td class="ChairmenName">Петренко О.В.</td>
</tr>
<tr>
  <td class="RegNumber"><a href="/Review/87654321">87654321</a></td>
  <td class="VRType">Рішення</td>
  <td class="RegDate">05.04.2026</td>
  <td class="LawDate">03.04.2026</td>
  <td class="CSType">Кримінальне</td>
  <td class="CaseNumber">199/2348/23</td>
  <td class="CourtName">Індустріальний районний суд м.Дніпропетровська</td>
  <td class="ChairmenName">Іваненко С.М.</td>
</tr>
<tr>
  <td class="RegNumber"><a href="/Review/11111111">11111111</a></td>
  <td class="VRType">Вирок</td>
  <td class="RegDate">01.04.2026</td>
  <td class="LawDate">28.03.2026</td>
  <td class="CSType">Кримінальне</td>
  <td class="CaseNumber">202/10407/22</td>
  <td class="CourtName">Жовтневий районний суд м.Дніпропетровська</td>
  <td class="ChairmenName">Сидоренко А.П.</td>
</tr>
</table>
</body>
</html>
"""

# HTML with no results
NO_RESULTS_HTML = """
<html><body>
<div>За заданими параметрами пошуку нічого не знайдено</div>
</body></html>
"""


def test_parser():
    print("=== Test: Parse sample HTML ===")
    results = parse_edrsr_html(SAMPLE_HTML)
    assert len(results) == 3, f"Expected 3 results, got {len(results)}"
    
    # Check first result
    r1 = results[0]
    assert r1["RegNumber"] == "12345678", f"RegNumber: {r1.get('RegNumber')}"
    assert r1["VRType"] == "Ухвала", f"VRType: {r1.get('VRType')}"
    assert r1["LawDate"] == "08.04.2026", f"LawDate: {r1.get('LawDate')}"
    assert r1["CaseNumber"] == "199/2348/23", f"CaseNumber: {r1.get('CaseNumber')}"
    assert r1["CourtName"] == "Індустріальний районний суд м.Дніпропетровська"
    assert r1["ChairmenName"] == "Петренко О.В."
    assert r1["_href"] == "/Review/12345678"
    print(f"  ✅ Result 1: {r1['VRType']} — {r1['CaseNumber']} ({r1['LawDate']})")
    
    # Check second result
    r2 = results[1]
    assert r2["VRType"] == "Рішення"
    assert r2["_href"] == "/Review/87654321"
    print(f"  ✅ Result 2: {r2['VRType']} — {r2['CaseNumber']} ({r2['LawDate']})")
    
    # Check third result (different case)
    r3 = results[2]
    assert r3["CaseNumber"] == "202/10407/22"
    assert r3["VRType"] == "Вирок"
    print(f"  ✅ Result 3: {r3['VRType']} — {r3['CaseNumber']} ({r3['LawDate']})")
    
    print("\n=== Test: Decision UID ===")
    # Simulate enriched decision
    r1["review_id"] = "12345678"
    uid1 = decision_uid(r1)
    assert uid1 == "edrsr_12345678", f"UID: {uid1}"
    print(f"  ✅ UID: {uid1}")
    
    # UID stability
    uid1_again = decision_uid(r1)
    assert uid1 == uid1_again, "UID should be stable"
    print(f"  ✅ UID is stable across calls")
    
    # Different decisions have different UIDs
    r2["review_id"] = "87654321"
    uid2 = decision_uid(r2)
    assert uid1 != uid2, "Different decisions should have different UIDs"
    print(f"  ✅ Different decisions → different UIDs")
    
    print("\n=== Test: No results detection ===")
    results_empty = parse_edrsr_html(NO_RESULTS_HTML)
    assert len(results_empty) == 0, f"Expected 0 results, got {len(results_empty)}"
    print(f"  ✅ Empty results correctly parsed")
    
    print("\n🎉 All tests passed!")


if __name__ == "__main__":
    test_parser()
