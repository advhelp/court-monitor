"""
Test the ЄДРСР HTML parser with sample data.
Run locally: python test_parser.py
"""
from edrsr_monitor import parse_edrsr_html, decision_uid

# Sample HTML that mimics ЄДРСР search results structure
SAMPLE_HTML = """
<html>
<body>
<div id="divresult">
<table id="tableresult">
<tbody>
<tr><th>Nr</th></tr>
<tr>
  <td class="RegNumber tr1"><a class="doc_text2" href="/Review/118948581" target="_blank">118948581</a></td>
  <td class="VRType tr1">Ухвала</td>
  <td class="RegDate tr1">09.05.2024</td>
  <td class="LawDate tr1"></td>
  <td class="CSType tr1">Цивільне</td>
  <td class="CaseNumber tr1">490/3823/24</td>
  <td class="CourtName tr1">Центральний районний суд м. Миколаєва</td>
  <td class="ChairmenName tr1">Гуденко О. А.</td>
</tr>
<tr>
  <td class="RegNumber tr1"><a class="doc_text2" href="/Review/128228354" target="_blank">128228354</a></td>
  <td class="VRType tr1">Рішення</td>
  <td class="RegDate tr1">09.06.2025</td>
  <td class="LawDate tr1">29.09.2025</td>
  <td class="CSType tr1">Цивільне</td>
  <td class="CaseNumber tr1">490/3823/24</td>
  <td class="CourtName tr1">Центральний районний суд м. Миколаєва</td>
  <td class="ChairmenName tr1">Гуденко О. А.</td>
</tr>
<tr>
  <td class="RegNumber tr1"><a class="doc_text2" href="/Review/129110090" target="_blank">129110090</a></td>
  <td class="VRType tr1">Ухвала</td>
  <td class="RegDate tr1">28.07.2025</td>
  <td class="LawDate tr1">28.07.2025</td>
  <td class="CSType tr1">Цивільне</td>
  <td class="CaseNumber tr1">490/3823/24</td>
  <td class="CourtName tr1">Миколаївський апеляційний суд</td>
  <td class="ChairmenName tr1">Самчишина Н. В.</td>
</tr>
</tbody>
</table>
</div>
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
    print("=== Test: Parse real ЄДРСР HTML ===")
    results = parse_edrsr_html(SAMPLE_HTML)
    assert len(results) == 3, f"Expected 3 results, got {len(results)}"
    
    # Check first result
    r1 = results[0]
    assert r1["RegNumber"] == "118948581", f"RegNumber: {r1.get('RegNumber')}"
    assert r1["VRType"] == "Ухвала", f"VRType: {r1.get('VRType')}"
    assert r1["RegDate"] == "09.05.2024", f"RegDate: {r1.get('RegDate')}"
    assert r1["CaseNumber"] == "490/3823/24", f"CaseNumber: {r1.get('CaseNumber')}"
    assert r1["CourtName"] == "Центральний районний суд м. Миколаєва"
    assert r1["ChairmenName"] == "Гуденко О. А."
    assert r1["_href"] == "/Review/118948581"
    print(f"  ✅ Result 1: {r1['VRType']} — {r1['CaseNumber']} ({r1['RegDate']})")
    
    # Check second result (Рішення with LawDate)
    r2 = results[1]
    assert r2["VRType"] == "Рішення"
    assert r2["LawDate"] == "29.09.2025"
    assert r2["_href"] == "/Review/128228354"
    print(f"  ✅ Result 2: {r2['VRType']} — {r2['CaseNumber']} ({r2['LawDate']})")
    
    # Check third result (different court - appeal)
    r3 = results[2]
    assert r3["CourtName"] == "Миколаївський апеляційний суд"
    assert r3["ChairmenName"] == "Самчишина Н. В."
    print(f"  ✅ Result 3: {r3['VRType']} — {r3['CourtName']}")
    
    print("\n=== Test: Decision UID ===")
    # Simulate enriched decision
    r1["review_id"] = "118948581"
    uid1 = decision_uid(r1)
    assert uid1 == "edrsr_118948581", f"UID: {uid1}"
    print(f"  ✅ UID: {uid1}")
    
    # UID stability
    uid1_again = decision_uid(r1)
    assert uid1 == uid1_again, "UID should be stable"
    print(f"  ✅ UID is stable across calls")
    
    # Different decisions have different UIDs
    r2["review_id"] = "128228354"
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
