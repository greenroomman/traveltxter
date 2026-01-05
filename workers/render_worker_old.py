from PIL import Image, ImageDraw, ImageFont

WIDTH, HEIGHT = 1080, 1080


def generate_deal_image(data, output_path):
    try:
        img = Image.new("RGB", (WIDTH, HEIGHT), "#F5F5F5")
        draw = ImageDraw.Draw(img)

        try:
            title = ImageFont.truetype("Montserrat-Bold.ttf", 72)
            body = ImageFont.truetype("Montserrat-Regular.ttf", 48)
            price_font = ImageFont.truetype("Montserrat-ExtraBold.ttf", 110)
        except:
            title = body = price_font = ImageFont.load_default()

        draw.text((100, 120), f"TO: {data['TO']}", fill="#111", font=title)
        draw.text((100, 220), f"FROM: {data['FROM']}", fill="#333", font=body)
        draw.text((100, 360), f"OUT: {data['OUT']}", fill="#333", font=body)
        draw.text((100, 440), f"IN: {data['IN']}", fill="#333", font=body)
        draw.text((100, 600), data["PRICE"], fill="#000", font=price_font)

        draw.text((100, 960), "TravelTxter", fill="#999", font=body)

        img.save(output_path, "PNG", optimize=True)
        return True

    except Exception as e:
        print("Render error:", e)
        return False
