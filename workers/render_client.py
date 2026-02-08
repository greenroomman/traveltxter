jobs:
  test-render:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      
      - name: Install dependencies
        run: pip install requests
      
      - name: Test renderer
        run: python workers/render_client.py
```

---

## VERIFICATION

After pushing, confirm logs show:
```
Testing health...
{'ok': True}
----------------------------------------
Rendering layout=AM theme=northern_lights London -> Keflavik 120326/180326 £159
{
    "graphic_url": "https://...",
    "layout": "AM",
    "ok": true,
    "theme": "northern_lights"
}
----------------------------------------
Rendering layout=PM theme=northern_lights London -> Keflavik 120326/180326 £159
{
    "graphic_url": "https://...",
    "layout": "PM",
    "ok": true,
    "theme": "northern_lights"
}
----------------------------------------

✅ All render tests passed
