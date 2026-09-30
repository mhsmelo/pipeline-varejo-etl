"""Dados no mesmo formato da DummyJSON (conferido em /products, /users e /carts)."""
import random

random.seed(42)
CATEGORIAS = ["beauty", "fragrances", "furniture", "groceries", "smartphones"]


def produtos(n=30):
    return [{
        "id": i,
        "title": f"Produto {i}",
        "description": f"Descrição do produto {i}",
        "category": CATEGORIAS[i % len(CATEGORIAS)],
        "price": round(random.uniform(5, 500), 2),
        "discountPercentage": round(random.uniform(0, 20), 2),
        "rating": round(random.uniform(1, 5), 2),
        "stock": random.randint(0, 150),
        "tags": ["tag"],
        **({"brand": f"Marca {i % 4}"} if i % 3 else {}),  # parte sem marca, como na API
        "sku": f"SKU-{i:03d}",
        "dimensions": {"width": 1.0, "height": 2.0, "depth": 3.0},
        "availabilityStatus": "In Stock",
        "reviews": [{"rating": 5, "comment": "Bom"}],
        "meta": {"createdAt": "2025-10-09T14:47:01.588Z"},
        "images": ["https://cdn/x.webp"],
    } for i in range(1, n + 1)]


def usuarios(n=20):
    return [{
        "id": i, "firstName": f"Nome{i}", "lastName": f"Sobrenome{i}",
        "email": f"Nome{i}@X.DummyJSON.com", "age": 18 + i * 2,
        "gender": "female" if i % 2 else "male", "birthDate": f"1990-{i % 12 + 1}-5",
        "address": {"address": "Rua 1", "city": "Phoenix", "state": "Mississippi",
                    "stateCode": "MS", "postalCode": "29112",
                    "coordinates": {"lat": -77.1, "lng": -92.0}, "country": "United States"},
    } for i in range(1, n + 1)]


def carrinhos(n=15, n_produtos=30, n_usuarios=20):
    lista = []
    for i in range(1, n + 1):
        itens = []
        for pid in random.sample(range(1, n_produtos + 1), 3):
            preco, qtd, desc = round(random.uniform(5, 500), 2), random.randint(1, 5), 10.0
            total = round(preco * qtd, 2)
            itens.append({"id": pid, "title": f"Produto {pid}", "price": preco, "quantity": qtd,
                          "total": total, "discountPercentage": desc,
                          "discountedTotal": round(total * 0.9, 2), "thumbnail": "x"})
        lista.append({"id": i, "products": itens, "userId": (i % n_usuarios) + 1,
                      "total": sum(x["total"] for x in itens),
                      "discountedTotal": sum(x["discountedTotal"] for x in itens),
                      "totalProducts": 3, "totalQuantity": sum(x["quantity"] for x in itens)})
    return lista


def api_fake():
    dados = {"produtos": produtos(), "clientes": usuarios(), "vendas": carrinhos()}
    return lambda entidade, session=None: dados[entidade]
