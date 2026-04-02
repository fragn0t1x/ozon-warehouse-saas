import type { Variant } from '@/lib/api/products';

const VISIBLE_CHARACTERISTICS = ['Цвет', 'Размер', 'Количество в упаковке', 'Кол-во в упаковке', 'Упаковка'];
type VariantPresentationSource = Pick<Variant, 'offer_id' | 'attributes' | 'pack_size'>;
type VariantGroupSource = VariantPresentationSource & { product_name: string };

export function getVisibleCharacteristicEntries(
  attributes?: Record<string, string>,
  packSize?: number
): Array<[string, string]> {
  const source = attributes || {};
  const result: Array<[string, string]> = [];

  for (const key of VISIBLE_CHARACTERISTICS) {
    if (!source[key]) {
      continue;
    }

    if (key === 'Упаковка' || key === 'Количество в упаковке' || key === 'Кол-во в упаковке') {
      if (result.some(([existingKey]) => existingKey === 'Количество в упаковке')) {
        continue;
      }
      result.push(['Количество в упаковке', source[key]]);
      continue;
    }

    result.push([key, source[key]]);
  }

  if (packSize && !result.some(([key]) => key === 'Количество в упаковке')) {
    result.push(['Количество в упаковке', `${packSize} шт`]);
  }

  return result;
}

export function getVariantColor(variant: Pick<Variant, 'attributes'>): string {
  return variant.attributes?.['Цвет'] || 'Без цвета';
}

export function getVariantSize(variant: Pick<Variant, 'attributes'>): string {
  return variant.attributes?.['Размер'] || 'Без размера';
}

export function getVariantSizeSortValue(size: string) {
  if (!size || size === 'Без размера') {
    return [999, 0, ''] as const;
  }

  const normalized = size.toLowerCase();
  const letterSizes: Record<string, number> = {
    xs: 1,
    s: 2,
    m: 3,
    l: 4,
    xl: 5,
    xxl: 6,
    xxxl: 7,
    '2xl': 6,
    '3xl': 7,
    '4xl': 8,
    '5xl': 9,
  };

  if (letterSizes[normalized] !== undefined) {
    return [1, letterSizes[normalized], normalized] as const;
  }

  if (size.includes('-')) {
    const first = Number.parseInt(size.split('-')[0], 10);
    if (!Number.isNaN(first)) {
      return [2, first, normalized] as const;
    }
  }

  const numeric = normalized.match(/\d+/);
  if (numeric) {
    return [2, Number.parseInt(numeric[0], 10), normalized] as const;
  }

  return [3, 0, normalized] as const;
}

export function getVariantPackLabel(variant: Pick<Variant, 'pack_size' | 'attributes'>): string {
  const raw =
    variant.attributes?.['Количество в упаковке'] ||
    variant.attributes?.['Кол-во в упаковке'] ||
    variant.attributes?.['Упаковка'];

  if (raw) {
    return raw;
  }

  return `${variant.pack_size} шт`;
}

export function getVariantVisibleCharacteristics(variant: Pick<Variant, 'attributes' | 'pack_size'>): Array<[string, string]> {
  return getVisibleCharacteristicEntries(variant.attributes, variant.pack_size);
}

export function getVariantDisplayTitle(variant: VariantPresentationSource): string {
  return `Артикул: ${variant.offer_id}`;
}

export function getVariantCharacteristicText(
  variant: VariantPresentationSource,
  options?: { withLabels?: boolean; delimiter?: string }
): string {
  const withLabels = options?.withLabels ?? true;
  const delimiter = options?.delimiter ?? ' • ';

  const entries = getVisibleCharacteristicEntries(variant.attributes, variant.pack_size);
  if (!entries.length) {
    return 'Без характеристик';
  }

  return entries
    .map(([key, value]) => (withLabels ? `${key}: ${value}` : value))
    .join(delimiter);
}

export function compareVariantPresentation(left: VariantPresentationSource, right: VariantPresentationSource): number {
  const leftColor = getVariantColor(left);
  const rightColor = getVariantColor(right);
  const colorCompare = leftColor.localeCompare(rightColor, 'ru', { sensitivity: 'base' });
  if (colorCompare !== 0) {
    return colorCompare;
  }

  const [leftPriority, leftNumber, leftLabel] = getVariantSizeSortValue(getVariantSize(left));
  const [rightPriority, rightNumber, rightLabel] = getVariantSizeSortValue(getVariantSize(right));

  if (leftPriority !== rightPriority) {
    return leftPriority - rightPriority;
  }
  if (leftNumber !== rightNumber) {
    return leftNumber - rightNumber;
  }

  const sizeCompare = leftLabel.localeCompare(rightLabel, 'ru', { sensitivity: 'base' });
  if (sizeCompare !== 0) {
    return sizeCompare;
  }

  if (left.pack_size !== right.pack_size) {
    return left.pack_size - right.pack_size;
  }

  return left.offer_id.localeCompare(right.offer_id, 'ru', { sensitivity: 'base' });
}

export function groupVariantEntries<T extends VariantGroupSource>(items: T[]) {
  const productMap = new Map<string, T[]>();

  items.forEach((item) => {
    const productName = item.product_name || 'Без названия';
    if (!productMap.has(productName)) {
      productMap.set(productName, []);
    }
    productMap.get(productName)!.push(item);
  });

  return Array.from(productMap.entries())
    .sort(([left], [right]) => left.localeCompare(right, 'ru', { sensitivity: 'base' }))
    .map(([productName, productItems]) => {
      const colorMap = new Map<string, T[]>();

      productItems.forEach((item) => {
        const color = getVariantColor(item) || 'Без цвета';
        if (!colorMap.has(color)) {
          colorMap.set(color, []);
        }
        colorMap.get(color)!.push(item);
      });

      return {
        productName,
        colors: Array.from(colorMap.entries())
          .sort(([left], [right]) => left.localeCompare(right, 'ru', { sensitivity: 'base' }))
          .map(([color, colorItems]) => {
            const sizeMap = new Map<string, T[]>();

            colorItems
              .slice()
              .sort(compareVariantPresentation)
              .forEach((item) => {
                const size = getVariantSize(item) || 'Без размера';
                if (!sizeMap.has(size)) {
                  sizeMap.set(size, []);
                }
                sizeMap.get(size)!.push(item);
              });

            return {
              color,
              sizes: Array.from(sizeMap.entries())
                .sort(([left], [right]) => {
                  const [leftPriority, leftNumber, leftLabel] = getVariantSizeSortValue(left);
                  const [rightPriority, rightNumber, rightLabel] = getVariantSizeSortValue(right);

                  if (leftPriority !== rightPriority) {
                    return leftPriority - rightPriority;
                  }
                  if (leftNumber !== rightNumber) {
                    return leftNumber - rightNumber;
                  }
                  return leftLabel.localeCompare(rightLabel, 'ru', { sensitivity: 'base' });
                })
                .map(([size, sizeItems]) => ({
                  size,
                  items: sizeItems.slice().sort(compareVariantPresentation),
                })),
            };
          }),
      };
    });
}
